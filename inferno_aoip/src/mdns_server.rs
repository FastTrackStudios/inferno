use searchfire::{
  broadcast::{BroadcasterBuilder, BroadcasterHandle, ServiceBuilder},
  dns::rr::Name,
  net::{IpVersion, TargetInterface},
};
use std::{net::IpAddr, sync::{Arc, RwLock}};

use crate::{device_info::DeviceInfo, utils::LogAndForget};
use crate::flows_tx::{FPP_MIN, FPP_MAX_ADVERTISED, MAX_CHANNELS_IN_FLOW};


pub struct DeviceMDNSResponder {
  handle: RwLock<Option<BroadcasterHandle>>,
  self_info: Arc<DeviceInfo>,
}

pub fn kv<T: std::fmt::Display>(key: &str, value: T) -> String {
  format!("{key}={value}")
}

pub fn service_type(st: &str) -> Name {
  Name::from_labels([st, "_udp", "local"].iter().map(|&s| s.as_bytes())).unwrap()
}

impl DeviceMDNSResponder {
  pub fn start(self_info: Arc<DeviceInfo>) -> Self {
    let hostname = Name::from_labels([self_info.friendly_hostname.as_bytes()]).unwrap();
    let bb = BroadcasterBuilder::new()
      //.loopback()
      .interface_v4(TargetInterface::Specific(self_info.ip_address))
      .add_service(
        ServiceBuilder::new(service_type("_netaudio-arc"), hostname.clone(), self_info.arc_port)
          .unwrap()
          .add_ip_address(IpAddr::V4(self_info.ip_address))
          .add_txt_truncated("arcp_vers=2.7.41")
          .add_txt_truncated("arcp_min=0.2.4")
          .add_txt_truncated("router_vers=4.0.2")
          .add_txt_truncated(kv("router_info", &self_info.board_name))
          .add_txt_truncated(kv("mf", &self_info.manufacturer))
          .add_txt_truncated(kv("model", &self_info.model_number))
          .ttl(4500)
          .build()
          .unwrap(),
      )
      .add_service(
        ServiceBuilder::new(service_type("_netaudio-cmc"), hostname, self_info.cmc_port)
          .unwrap()
          .add_ip_address(IpAddr::V4(self_info.ip_address))
          .add_txt_truncated(kv("id", &hex::encode(self_info.factory_device_id)))
          .add_txt_truncated(kv("process", self_info.process_id))
          .add_txt_truncated("cmcp_vers=1.2.0")
          .add_txt_truncated("cmcp_min=1.0.0")
          .add_txt_truncated("server_vers=4.0.2")
          .add_txt_truncated("channels=0x6000004d") // ???
          .add_txt_truncated(kv("mf", &self_info.manufacturer))
          .add_txt_truncated(kv("model", &self_info.model_number))
          .add_txt_truncated("") // really needed?
          .add_txt_truncated("") // really needed?
          .build()
          .unwrap(),
      );
    
    let handle = bb.build(IpVersion::V4)
      .unwrap()
      .run_in_background();
      // TODO it doesn't work when there is no default gateway in routing table
      // thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value: MultiIpIoError(V4(Os { code: 101, kind: NetworkUnreachable, message: "Network is unreachable" }))', inferno_aoip/src/mdns_server.rs:55:6

    Self { handle: RwLock::new(Some(handle)), self_info }
  }

  pub fn add_tx_channel(&self, index: usize) {
    let self_info = &*self.self_info;
    let service = |ch_name: &str, default: bool| {
      let name = Name::from_labels([format!("{}@{}", ch_name, self_info.friendly_hostname).as_bytes()]).unwrap();
      let mut b = ServiceBuilder::new(service_type("_netaudio-chan"), name, self_info.flows_control_port)
        .unwrap()
        .add_ip_address(IpAddr::V4(self_info.ip_address))
        .add_txt_truncated("txtvers=2")
        .add_txt_truncated("dbcp1=0x1102")
        .add_txt_truncated("dbcp=0x1004")
        .add_txt_truncated(kv("id", index+1))
        .add_txt_truncated(kv("rate", self_info.sample_rate))
        .add_txt_truncated(format!("pcm={} {:x}", self_info.bits_per_sample/8, self_info.pcm_type))
        .add_txt_truncated(kv("enc", self_info.bits_per_sample))
        .add_txt_truncated(kv("en", self_info.bits_per_sample))
        .add_txt_truncated(kv("latency_ns", self_info.latency_ns))
        .add_txt_truncated(format!("fpp={},{}", FPP_MAX_ADVERTISED, FPP_MIN))
        .add_txt_truncated(kv("nchan", MAX_CHANNELS_IN_FLOW.min(self_info.tx_channels.len() as u16)));
      if default {
        b = b.add_txt_truncated("default");
      }
      b.build().unwrap()
    };
    let txch = &self_info.tx_channels[index];
    let handle = self.handle.read().unwrap();
    match handle.as_ref() {
      Some(handle) => {
        handle.add_service(service(&txch.factory_name, true)).log_and_forget();
        let friendly_name_locked = txch.friendly_name.read();
        let friendly_name = friendly_name_locked.unwrap();
        if txch.factory_name != *friendly_name {
          handle.add_service(service(&friendly_name, false)).log_and_forget();
        }
      },
      None => {
        log::error!("BUG: trying to add channel using BroadcasterHandle after it was shut down");
      }
    };
  }
  
  pub fn remove_tx_channel(&self, index: usize) {
    let self_info = &*self.self_info;
    let remove = |ch_name: &str| {
      let name = Name::from_labels([format!("{}@{}", ch_name, self_info.friendly_hostname).as_bytes()]).unwrap();
      match self.handle.read().unwrap().as_ref() {
        Some(handle) => {
          handle.remove_named_service(service_type("_netaudio-chan"), name).log_and_forget();
        },
        None => {
          log::error!("BUG: trying to remove channel using BroadcasterHandle after it was shut down");
        }
      }
    };
    let txch = &self_info.tx_channels[index];
    remove(&txch.factory_name);
    let friendly_name_locked = txch.friendly_name.read();
    let friendly_name = friendly_name_locked.unwrap();
    if txch.factory_name != *friendly_name {
      remove(&friendly_name);
    }
  }

  pub fn shutdown_and_join(&self) {
    self.handle.write().unwrap().take().expect("shutting down more than once").shutdown().log_and_forget();
  }
}
