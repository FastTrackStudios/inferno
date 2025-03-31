use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::{net::Ipv4Addr, sync::Arc};

use itertools::Itertools;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::device_info::DeviceInfo;
use crate::device_server::flows_tx::FPP_MAX_ADVERTISED;
use crate::mdns_client::MdnsClient;
use crate::state_storage::StateStorage;
use crate::utils::LogAndForget;

use super::flows_tx::{FlowsTransmitter, MAX_FLOWS};
use super::flows_tx::{FlowInfo as TXFlowInfo};
use super::mdns_server::DeviceMDNSResponder;

#[derive(Serialize, Deserialize)]
struct Bundle {
  local_channel_indices: Vec<Option<usize>>,
}

#[derive(Serialize, Deserialize)]
struct Bundles {
  bundles: Vec<Option<Bundle>>,
}

pub struct TransmitMulticasts {
  bundles: Arc<Mutex<Bundles>>,
  should_work: Arc<AtomicBool>,
  state_storage: Arc<StateStorage>,
  self_info: Arc<DeviceInfo>,
  flows_tx: Arc<Mutex<Option<FlowsTransmitter>>>,
  mdns_server: Arc<DeviceMDNSResponder>,
  mdns_client: Arc<MdnsClient>,
}

impl TransmitMulticasts {
  pub fn new(
    state_storage: Arc<StateStorage>,
    self_info: Arc<DeviceInfo>,
    flows_tx: Arc<Mutex<Option<FlowsTransmitter>>>,
    mdns_server: Arc<DeviceMDNSResponder>,
    mdns_client: Arc<MdnsClient>,
  ) -> Self {
    let mut bundles = state_storage.load("tx_multicasts").unwrap_or_else(|_|Bundles { bundles: (0..MAX_FLOWS).map(|_|None).collect_vec() });
    if bundles.bundles.len() < (MAX_FLOWS as usize) {
      bundles.bundles.extend((bundles.bundles.len()..MAX_FLOWS as usize).map(|_|None));
    }
    Self {
      bundles: Arc::new(Mutex::new(bundles)),
      should_work: Arc::new(true.into()),
      state_storage,
      self_info,
      flows_tx,
      mdns_server,
      mdns_client
    }
  }
  pub async fn add_flow(&self, flow_index: usize, channel_indices: Vec<Option<usize>>) {
    info!("adding flow index {flow_index} with local channel indices {channel_indices:?}");
    let bytes_per_sample = (self.self_info.bits_per_sample/8).try_into().unwrap();
    let dst = {
      let mut bundles = self.bundles.lock().await;
      assert!(bundles.bundles[flow_index].is_none());
      let mut flows_tx = self.flows_tx.lock().await;
      let (dst_addr, dst_port) = if let Some(tx) = flows_tx.as_mut() {
        let (dst_addr, dst_port) = tx.random_multicast_destination();
        let flow_info = TXFlowInfo {
          rx_hostname: None,
          rx_flow_name: None,
          dst_addr,
          dst_port,
          local_channel_indices: channel_indices.clone(),
        };
        tx.add_flow(
          flow_info,
          FPP_MAX_ADVERTISED.try_into().unwrap() /* TODO */,
          (self.self_info.bits_per_sample/8).try_into().unwrap(),
          Some(flow_index.try_into().unwrap()),
          true
        ).await.log_and_forget();
        info!("added multicast flow, waiting grace period...");
        (dst_addr, dst_port)
      } else {
        error!("BUG: TransmitMulticasts::add_flow called but flows_tx is None. this flow will start working only after you restart Inferno");
        (Ipv4Addr::UNSPECIFIED, 0)
      };
      bundles.bundles[flow_index] = Some(Bundle { local_channel_indices: channel_indices.clone() });
      (dst_addr, dst_port)
    };
    self.save_state().await;
    if !dst.0.is_unspecified() {
      let mdns_client = self.mdns_client.clone();
      let mdns_server = self.mdns_server.clone();
      let flows_tx = self.flows_tx.clone();
      let should_work = self.should_work.clone();
      tokio::spawn(async move {
        let mut dst_addr = dst.0;
        let mut dst_port = dst.1;
        loop {
          let mut ok = true;
          ok &= !mdns_client.is_multicast_ip_already_used(dst_addr).await.unwrap_or(true);
          if ok {
            mdns_server.reserve_multicast_ip(dst_addr);
            ok &= !mdns_client.is_multicast_ip_already_used(dst_addr).await.unwrap_or(true);
          }
          {
            let mut flows_tx_opt = flows_tx.lock().await;
            if let Some(flows_tx) = flows_tx_opt.as_mut() {
              if !should_work.load(std::sync::atomic::Ordering::SeqCst) {
                // abort whatever is being done, the flows_tx will be destroyed soon...
                break;
              }
              if ok {
                info!("no multicast conflict detected, activating transmitter");
                flows_tx.activate_multicast_flow(flow_index.try_into().unwrap());
                break;
              } else {
                warn!("multicast address conflict detected: {dst_addr:?}, retrying");
                flows_tx.remove_multicast_flow(flow_index.try_into().unwrap()).await.log_and_forget();
                (dst_addr, dst_port) = flows_tx.random_multicast_destination();

                // note: the following add_flow must happen after remove_multicast_flow, WITHOUT flows_tx being unlocked in the meantime!
                // otherwise race condition may happen

                // TODO: DRY
                let flow_info = TXFlowInfo {
                  rx_hostname: None,
                  rx_flow_name: None,
                  dst_addr,
                  dst_port,
                  local_channel_indices: channel_indices.clone(),
                };
                flows_tx.add_flow(
                  flow_info,
                  FPP_MAX_ADVERTISED.try_into().unwrap() /* TODO */,
                  bytes_per_sample,
                  Some(flow_index.try_into().unwrap()),
                  true
                ).await.log_and_forget();
              }
            } else {
              error!("trying to activate multicast flow but we have no flows transmitter active");
              break;
            }
          }
        }
      });
    }
  }
  pub async fn remove_flow(&self, flow_index: usize) {
    let mut bundles = self.bundles.lock().await;
    bundles.bundles[flow_index] = None;
    self.save_state().await;
  }
  pub async fn shutdown(&self) {
    let _flows_tx_opt = self.flows_tx.lock().await;
    self.should_work.store(false, std::sync::atomic::Ordering::SeqCst);
  }
  pub async fn save_state(&self) {
    // FIXME: Encountered error Error { inner: UnsupportedNone }
    self.state_storage.save("tx_multicasts", &*self.bundles.lock().await).log_and_forget();
  }
}
