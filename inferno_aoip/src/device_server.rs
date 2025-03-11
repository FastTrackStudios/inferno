use crate::channels_subscriber::{ChannelsBuffering, ChannelsSubscriber, ExternalBuffering, OwnedBuffering};
use crate::flows_tx::FlowsTransmitter;
use crate::info_mcast_server::{MulticastMessage, PeaksCallback};
use crate::mdns_client::MdnsClient;
use crate::media_clock::{async_clock_receiver_to_realtime, make_shared_media_clock, start_clock_receiver, ClockReceiver};
use crate::peaks::peaks_of_buffers;
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
use std::thread::sleep;
use std::{env, os};
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use std::net::IpAddr;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast as broadcast_queue, mpsc, watch};

use crate::device_info::{Channel, DeviceInfo};
use crate::{common::*, AtomicSample, MediaClock, RealTimeClockReceiver};
use crate::flows_control_server::FlowInfo as TXFlowInfo;

const PEAKS_BUFFER_LEN: usize = 24000;
const PEAKS_ITER_SLEEP: Duration = Duration::from_millis(100);

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
  rx_peaks_supplier: Arc<RwLock<Box<dyn Fn() -> Vec<u8> + Send + Sync>>>,
  tx_peaks_supplier: Arc<RwLock<Box<dyn Fn() -> Vec<u8> + Send + Sync>>>,
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
    //let tx_peaks = Arc::new((0..self_info.tx_channels.len()).map(|_|AtomicSample::new(0)).collect_vec());
    //let rx_peaks = Arc::new((0..self_info.rx_channels.len()).map(|_|AtomicSample::new(0)).collect_vec());

    let rx_peaks_supplier: Arc<RwLock<Box<dyn Fn() -> Vec<u8> + Send + Sync>>> = Arc::new(RwLock::new(Box::new(|| -> Vec<u8> {vec![]})));
    let tx_peaks_supplier: Arc<RwLock<Box<dyn Fn() -> Vec<u8> + Send + Sync>>> = Arc::new(RwLock::new(Box::new(|| -> Vec<u8> {vec![]})));
    let txps = tx_peaks_supplier.clone();
    let rxps = rx_peaks_supplier.clone();
    tasks.append(&mut vec![
      tokio::spawn(crate::arc_server::run_server(
        self_info.clone(),
        mcast_tx.clone(),
        channels_sub_rx.clone(),
        tx_flows_info.clone(),
        shdn_recv1,
      )),
      tokio::spawn(crate::cmc_server::run_server(self_info.clone(), shdn_recv2)),
      tokio::spawn(crate::info_mcast_server::run_server(self_info.clone(), mcast_rx, shared_media_clock.clone(), channels_sub_rx, Box::new(move || {
        ((rxps.read().unwrap())(), (txps.read().unwrap())())
      }), shdn_recv3)),
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
      rx_peaks_supplier,
      tx_peaks_supplier,
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
    let rbs = buffering.ring_buffers.clone();
    *self.rx_peaks_supplier.write().unwrap() = Box::new(move || peaks_of_buffers(&rbs));
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
    let rb_outputs: Vec<_> = tx_channels_buffers.iter().map(|par| ring_buffer::wrap_external_source(par, 0)).collect();
    let rbs = rb_outputs.iter().map(|rbo|rbo.shared().clone()).collect_vec();
    *self.tx_peaks_supplier.write().unwrap() = Box::new(move || peaks_of_buffers(&rbs));
    self.transmit(Some(start_time_rx), rb_outputs, current_timestamp, Some(on_transfer)).await;
  }
  async fn transmit<P: ProxyToSamplesBuffer + Send + Sync + 'static>(&mut self, start_time_rx: Option<tokio::sync::oneshot::Receiver<Clock>>, rb_outputs: Vec<RBOutput<Sample, P>>, current_timestamp: Arc<AtomicUsize>, on_transfer: Option<Box<dyn Fn() + Send>>) {
    let clock_rx = self.clock_receiver.subscribe();
    
    let (flows_tx_handle, flows_tx_thread) = FlowsTransmitter::start(self.self_info.clone(), clock_rx, rb_outputs.clone(), start_time_rx, current_timestamp.clone(), on_transfer);
    let (shutdown_send, shutdown_recv) = broadcast_queue::channel(16);
    let flows_control_task = tokio::spawn(crate::flows_control_server::run_server(self.self_info.clone(), flows_tx_handle, self.tx_flows_info.clone(), shutdown_recv));
    //let peaks_work = Arc::new(AtomicBool::new(true));
    //let peaks_work1 = peaks_work.clone();
    //let peaks = self.tx_peaks.clone();
    //let media_clock = self.shared_media_clock.clone();
    //let sample_rate = self.self_info.sample_rate;
    // TODO
    /* let peaks_thread = std::thread::Builder::new().name("peaks of TX".to_owned()).spawn(move || {
      let mut buf = vec![0 as Sample; PEAKS_BUFFER_LEN];
      while peaks_work.load(Ordering::Relaxed) {
        sleep(PEAKS_ITER_SLEEP);
        let read_from = 0; // TODO doesn't work because unconditional_read()==true in ALSA plugin
        for (chi, rbout) in rb_outputs.iter().enumerate() {
          let readable_until = rbout.readable_until();
          let to_read = readable_until.wrapping_sub(read_from).min(PEAKS_BUFFER_LEN);
          let start_timestamp = readable_until.wrapping_sub(to_read);
          rbout.read_at(start_timestamp, &mut buf);
          let samples = &buf[0..to_read];
          let peak = samples.iter().map(|n|n.saturating_abs()).max().unwrap_or(0);
          peaks[chi].fetch_max(peak, Ordering::Relaxed);
        }
      }
    }).unwrap(); */
    self.tx_shutdown_todo = Some(async move {
      //peaks_work1.store(false, Ordering::Relaxed);
      shutdown_send.send(()).unwrap();
      flows_control_task.await.unwrap();
      flows_tx_thread.join().unwrap();
      //peaks_thread.join().unwrap();
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
