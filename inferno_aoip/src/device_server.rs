use crate::channels_subscriber::{ChannelsBuffering, ChannelsSubscriber, ExternalBuffering, OwnedBuffering};
use crate::flows_tx::FlowsTransmitter;
use crate::info_mcast_server::MulticastMessage;
use crate::mdns_client::MdnsClient;
use crate::media_clock::{async_clock_receiver_to_realtime, make_shared_media_clock, start_clock_receiver, ClockReceiver};
use crate::protocol::flows_control;
use crate::real_time_box_channel::RealTimeBoxReceiver;
use crate::samples_collector::{RealTimeSamplesReceiver, SamplesCallback, SamplesCollector};
use crate::settings::Settings;
use crate::state_storage::StateStorage;
use crate::ring_buffer::{self, ExternalBuffer, ExternalBufferParameters, OwnedBuffer, ProxyToBuffer, ProxyToSamplesBuffer, RBInput, RBOutput};
use atomic::Atomic;
use futures::future::Join;
use futures::{Future, FutureExt};
use itertools::Itertools;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use usrvclock::ClockOverlay;

use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::Write;
use std::mem::size_of;
use std::{env, os};
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex, RwLock};

use std::net::IpAddr;
use std::time::Instant;
use tokio::sync::{broadcast as broadcast_queue, mpsc, watch};

use crate::device_info::{Channel, DeviceInfo};
use crate::{common::*, MediaClock, RealTimeClockReceiver};
use crate::flows_control_server::FlowInfo as TXFlowInfo;


pub struct DeviceServer {
  pub self_info: Arc<DeviceInfo>,
  ref_instant: Instant,
  state_storage: Arc<StateStorage>,
  clock_receiver: ClockReceiver,
  shared_media_clock: Arc<RwLock<MediaClock>>,
  mdns_client: Arc<MdnsClient>,
  mcast_tx: mpsc::Sender<MulticastMessage>,
  channels_sub_tx: watch::Sender<Option<Arc<ChannelsSubscriber>>>,
  tx_flows_info: Arc<RwLock<Vec<Option<TXFlowInfo>>>>,
  //tx_inputs: Vec<RBInput<Sample, P>>,
  //tasks: Vec<JoinHandle<()>>,
  shutdown_todo: Pin<Box<dyn Future<Output = ()> + Send>>,
  tx_shutdown_todo: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
  rx_shutdown_todo: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl DeviceServer {
  pub async fn start(settings: Settings) -> Self {
    let self_info = Arc::new(settings.self_info);
    let state_storage = Arc::new(StateStorage::new(&self_info));
    let ref_instant = Instant::now();

    let (shutdown_send, shdn_recv1) = broadcast_queue::channel(16);
    let shdn_recv2 = shutdown_send.subscribe();
    let shdn_recv3 = shutdown_send.subscribe();
    let mdns_handle = crate::mdns_server::start_server(self_info.clone());

    let mdns_client = Arc::new(crate::mdns_client::MdnsClient::new(self_info.ip_address));
    let (mcast_tx, mcast_rx) = mpsc::channel(100);

    info!("clock path: {:?}", settings.clock_path);
    let clock_receiver = start_clock_receiver(settings.clock_path.clone());

    info!("waiting for clock");
    let shared_media_clock = make_shared_media_clock(&clock_receiver).await;
    info!("clock ready");

    let mut tasks = vec![];

    let (channels_sub_tx, channels_sub_rx) = watch::channel(None);
    let tx_flows_info: Arc<RwLock<Vec<Option<TXFlowInfo>>>> = Default::default();

    tasks.append(&mut vec![
      tokio::spawn(crate::arc_server::run_server(
        self_info.clone(),
        channels_sub_rx.clone(),
        tx_flows_info.clone(),
        shdn_recv1,
      )),
      tokio::spawn(crate::cmc_server::run_server(self_info.clone(), shdn_recv2)),
      tokio::spawn(crate::info_mcast_server::run_server(self_info.clone(), mcast_rx, shared_media_clock.clone(), channels_sub_rx, shdn_recv3)),
    ]);

    info!("all common tasks spawned");

    let shutdown_todo = async move {
      shutdown_send.send(()).unwrap();
      mdns_handle.shutdown().unwrap();
      for task in tasks {
        task.await.unwrap();
      }
    }.boxed();

    Self {
      self_info,
      ref_instant,
      state_storage,
      clock_receiver,
      shared_media_clock,
      mdns_client,
      mcast_tx,
      channels_sub_tx,
      tx_flows_info,
      //tasks,
      //tx_inputs,
      shutdown_todo,
      rx_shutdown_todo: None,
      tx_shutdown_todo: None,
    }
  }

  pub async fn receive_with_callback(&mut self, samples_callback: SamplesCallback) {
    let (col, col_fut) = SamplesCollector::<OwnedBuffer<Atomic<Sample>>>::new_with_callback(self.self_info.clone(), Box::new(samples_callback));
    let tasks = vec![tokio::spawn(col_fut)];
    let buffering = OwnedBuffering::new(524288 /*TODO*/, 4800 /*TODO*/, Arc::new(col));
    self.receive(tasks, None, buffering, Default::default(), None).await;
  }
  pub async fn receive_realtime(&mut self) -> RealTimeSamplesReceiver<OwnedBuffer<Atomic<Sample>>> {
    let (col, col_fut, rt_recv) = SamplesCollector::new_realtime(self.self_info.clone(), self.get_realtime_clock_receiver());
    let tasks = vec![tokio::spawn(col_fut)];
    let buffering = OwnedBuffering::new(524288 /*TODO*/, 4800 /*TODO*/, Arc::new(col));
    self.receive(tasks, None, buffering, Default::default(), None).await;

    rt_recv
  }
  pub async fn receive_to_external_buffer(&mut self, rx_channels_buffers: Vec<ExternalBufferParameters<Sample>>, start_time_rx: tokio::sync::oneshot::Receiver<Clock>, current_timestamp: Arc<AtomicUsize>, on_transfer: Box<dyn Fn() + Send>) {
    let buffering = ExternalBuffering::new(rx_channels_buffers, 4800 /*TODO*/);
    self.receive(vec![], Some(start_time_rx), buffering, current_timestamp, Some(on_transfer)).await;
  }
  async fn receive<P: ProxyToSamplesBuffer + Send + Sync + 'static, B: ChannelsBuffering<P> + Send + Sync + 'static>(&mut self, mut tasks: Vec<JoinHandle<()>>, start_time_rx: Option<tokio::sync::oneshot::Receiver<Clock>>, channels_buffering: B, current_timestamp: Arc<AtomicUsize>, on_transfer: Option<Box<dyn Fn() + Send>>) {
    let (srx1, srx2) = if let Some(in_rx) = start_time_rx {
      let (stx1, srx1) = tokio::sync::oneshot::channel::<Clock>();
      let (stx2, srx2) = tokio::sync::oneshot::channel::<Clock>();
      tokio::spawn(async {
        if let Ok(v) = in_rx.await {
          let _ = stx1.send(v);
          let _ = stx2.send(v);
        }
      });
      (Some(srx1), Some(srx2))
    } else {
      (None, None)
    };
    let (flows_rx_handle, flows_rx_thread) = crate::flows_rx::FlowsReceiver::start(self.self_info.clone(), self.get_realtime_clock_receiver(), self.ref_instant, srx1, current_timestamp, on_transfer);
    let flows_rx_handle = Arc::new(flows_rx_handle);
    let (channels_sub_handle, channels_sub_worker) = ChannelsSubscriber::new(
      self.self_info.clone(),
      self.shared_media_clock.clone(),
      flows_rx_handle.clone(),
      self.mdns_client.clone(),
      self.mcast_tx.clone(),
      channels_buffering,
      self.state_storage.clone(),
      srx2,
      self.ref_instant,
    );
    let channels_sub_handle = Arc::new(channels_sub_handle);
    let _ = self.channels_sub_tx.send(Some(channels_sub_handle.clone()));

    tasks.push(tokio::spawn(channels_sub_worker));

    let shutdown_todo = async move {
      flows_rx_handle.shutdown().await;
      channels_sub_handle.shutdown().await;
      flows_rx_thread.join().unwrap();
      for task in tasks {
        task.await.unwrap();
      }
    }.boxed();
    self.rx_shutdown_todo = Some(shutdown_todo);
  }
  pub async fn stop_receiver(&mut self) {
    let _ = self.channels_sub_tx.send(None);
    self.rx_shutdown_todo.take().unwrap().await;
  }

  pub async fn transmit_from_external_buffer(&mut self, tx_channels_buffers: Vec<ExternalBufferParameters<Sample>>, start_time_rx: tokio::sync::oneshot::Receiver<Clock>, current_timestamp: Arc<AtomicUsize>, on_transfer: Box<dyn Fn() + Send>) {
    let rb_outputs = tx_channels_buffers.iter().map(|par| ring_buffer::wrap_external_source(par, 0)).collect();
    self.transmit(Some(start_time_rx), rb_outputs, current_timestamp, Some(on_transfer)).await;
  }
  async fn transmit<P: ProxyToSamplesBuffer + Send + Sync + 'static>(&mut self, start_time_rx: Option<tokio::sync::oneshot::Receiver<Clock>>, rb_outputs: Vec<RBOutput<Sample, P>>, current_timestamp: Arc<AtomicUsize>, on_transfer: Option<Box<dyn Fn() + Send>>) {
    let clock_rx = self.clock_receiver.subscribe();
    
    let (flows_tx_handle, flows_tx_thread) = FlowsTransmitter::start(self.self_info.clone(), clock_rx, rb_outputs, start_time_rx, current_timestamp.clone(), on_transfer);
    let (shutdown_send, shutdown_recv) = broadcast_queue::channel(16);
    let flows_control_task = tokio::spawn(crate::flows_control_server::run_server(self.self_info.clone(), flows_tx_handle, self.tx_flows_info.clone(), shutdown_recv));
    self.tx_shutdown_todo = Some(async move {
      shutdown_send.send(()).unwrap();
      flows_control_task.await.unwrap();
      flows_tx_thread.join().unwrap();
    }.boxed());
  }
  pub async fn stop_transmitter(&mut self) {
    self.tx_shutdown_todo.take().unwrap().await;
  }


  pub fn get_realtime_clock_receiver(&self) -> RealTimeClockReceiver {
    async_clock_receiver_to_realtime(self.clock_receiver.subscribe(), self.shared_media_clock.read().unwrap().get_overlay().clone())
  }

  /* pub fn take_tx_inputs(&mut self) -> Vec<RBInput<Sample, P>> {
    unimplemented!()
    //std::mem::take(&mut self.tx_inputs)
  } */

  pub async fn shutdown(self) {
    info!("shutting down");
    if let Some(todo) = self.rx_shutdown_todo {
      todo.await;
    }
    if let Some(todo) = self.tx_shutdown_todo {
      todo.await;
    }
    self.shutdown_todo.await;
    self.clock_receiver.stop().await.unwrap();
    info!("shutdown ok");
  }
}
