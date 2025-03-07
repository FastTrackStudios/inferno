use crate::channels_subscriber::ChannelsSubscriber;
use crate::common::*;
use crate::byte_utils::*;
use crate::net_utils::UdpSocketWrapper;
use crate::protocol::mcast::make_packet;
use crate::MediaClock;
use crate::{byte_utils::write_str_to_buffer, device_info::DeviceInfo};
use std::sync::atomic::Ordering;
use std::sync::RwLock;
use std::{
  net::{IpAddr, Ipv4Addr, SocketAddr},
  sync::Arc,
  time::Duration,
};
use bytebuffer::ByteBuffer;
use itertools::Itertools;
use tokio::sync::watch;
use tokio::time::interval;
use tokio::{
  select,
  sync::{broadcast::Receiver as BroadcastReceiver, mpsc},
  time::MissedTickBehavior,
};

const SEND_BUFFER_SIZE: usize = 1500;
const DST_PORT_HEARTBEAT: u16 = 8708;
const DST_PORT_DEVICE_INFO: u16 = 8702;

pub struct MulticastMessage {
  pub start_code: u16,
  pub opcode: [u8; 8],
  pub content: Vec<u8>,
}

struct Multicaster<'s> {
  self_info: &'s DeviceInfo,
  pub server: UdpSocketWrapper,
  seqnum: u16,
  vendor: [u8; 8],
  firmware_version_bytes: [u8; 4],
  product_version_bytes: [u8; 4],
  device_info_destination: SocketAddr,
  heartbeat_destination: SocketAddr,
  send_buffer: [u8; SEND_BUFFER_SIZE],
  clock: Arc<RwLock<MediaClock>>,
  channels_subscriber: Option<Arc<ChannelsSubscriber>>,
}

impl<'s> Multicaster<'s> {
  pub fn new(self_info: &'s DeviceInfo, server: UdpSocketWrapper, clock: Arc<RwLock<MediaClock>>) -> Multicaster {
    let patch_version = env!("CARGO_PKG_VERSION_PATCH").parse::<u16>().unwrap();
    let mut r = Multicaster {
      self_info,
      server,
      seqnum: 1,
      vendor: [32; 8],
      firmware_version_bytes: [4, 1, 6, 2],
      product_version_bytes: [
        env!("CARGO_PKG_VERSION_MAJOR").parse::<u8>().unwrap(),
        env!("CARGO_PKG_VERSION_MINOR").parse::<u8>().unwrap(),
        H(patch_version),
        L(patch_version)
      ],
      device_info_destination: SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(224, 0, 0, 231)),
        DST_PORT_DEVICE_INFO,
      ),
      heartbeat_destination: SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(224, 0, 0, 233)),
        DST_PORT_HEARTBEAT,
      ),
      send_buffer: [0; SEND_BUFFER_SIZE],
      clock,
      channels_subscriber: None,
    };
    write_str_to_buffer(&mut r.vendor, 0, 8, &self_info.vendor_string);
    return r;
  }

  pub fn should_work(&self) -> bool {
    return self.server.should_work();
  }

  async fn send(&mut self, dst: SocketAddr, start_code: u16, opcode: [u8; 8], content: &[u8]) {
    let pkt = make_packet(
      &mut self.send_buffer,
      start_code,
      self.seqnum,
      self.self_info.process_id,
      self.self_info.factory_device_id,
      self.vendor,
      opcode,
      content,
    );
    self.seqnum = self.seqnum.wrapping_add(1);
    self.server.send(&dst, pkt).await;
  }

  async fn send_board_info(&mut self) {
    let mut content = [0u8; 200];
    // Firmware version:
    content[0..4].copy_from_slice(&[4, 1, 0, 6]);
    content[0x23] = 2;
    // Hardware version:
    content[4..8].copy_from_slice(&[4, 1, 0, 3]);
    content[0x27] = 1;
    // Boot version:
    content[0x28..0x2c].copy_from_slice(&[1, 0, 0, 0]);

    // flags of supported features:
    // 0x14: AES67, Device Lock
    //       0x01 - ??? (was 1)
    //       0x04 - supports AES67
    //       0x08 - is lockable
    // 0x15: ??? (was 0x50)
    // 0x16:
    //       0x10 - has Manufacturer name
    //       0x40 - Network is configurable (supports static addressing)
    // 0x17: Identify device, Sample rate & encoding configuration (was 0xdb)
    content[0x14] = 0;
    content[0x15] = 0; // XXX
    content[0x16] = 0x10;
    content[0x17] = 0;

    content[0xbb] = 0x1f; // if 0, device is flooded with info multicast requests around 1 per second
    /* content[0xbf] = 5;
    content[0xc3] = 3;
    content[0xc7] = 3; */
    write_str_to_buffer(&mut content, 12, 8, &self.self_info.board_name);
    write_str_to_buffer(&mut content, 0x38, 16, &self.self_info.board_name);

    self
      .send(self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x60, 0, 0, 0, 0], &content)
      .await;
  }

  async fn send_product_info(&mut self) {
    let mut content = [0; 336];
    write_str_to_buffer(&mut content, 0, 8, &self.self_info.manufacturer);
    write_str_to_buffer(&mut content, 8, 8, &self.self_info.board_name);
    write_str_to_buffer(&mut content, 0x2c, 16, &self.self_info.manufacturer);
    write_str_to_buffer(&mut content, 0xac, 16, &self.self_info.model_name);
    // version number:
    content[0x12c..0x130].copy_from_slice(&self.product_version_bytes);

    self
      .send(self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0xc0, 0, 0, 0, 0], &content)
      .await;
  }

  async fn send_heartbeat(&mut self) {
    let ctr = self.seqnum;
    let mut bytes = ByteBuffer::new();
    bytes.set_endian(bytebuffer::Endian::BigEndian);
 
    let freq_offset_opt = self.clock.read().unwrap().get_overlay().as_ref().map(|clkovl| {
      let freq_offset_f = (clkovl.freq_scale_including_hw() * 1_000_000_000f64).round();
      if i32::MIN as f64 <= freq_offset_f && freq_offset_f <= i32::MAX as f64 {
        Some(freq_offset_f as i32)
      } else {
        None
      }
    }).flatten();

    if let Some(freq_offset) = freq_offset_opt {
      bytes.write_u16(16); // length of this part
      bytes.write_u16(0x8001); // type
      bytes.write_u16(4); // ???
      bytes.write_u16(4); // maybe content length???
      bytes.write_u16(ctr);
      bytes.write_u16(0);
      bytes.write_i32(freq_offset);
      debug!("freq offset {freq_offset}/1000 ppm");

      /* bytes.write_bytes(&[
        0x00, 0x24, 0x80, 0x00,
        0x00, 0x04, 0x00, 0x04, H(ctr), L(ctr), 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01, 0x00, 0x10,
        /* TX Bps, 4B: */ 0x00, 0x00, 0x01, 0xde, /* RX Bps, 4B: */ 0x00, 0x07, 0xdf, 0x17,
        /* TX errors, 4B: */ 0x00, 0x00, 0x00, 0x02, /* RX errors, 4B: */ 0x00, 0x00, 0x00, 0x07,
      ]); // network statistics */
  
      bytes.write_bytes(&[
        /* 24 + total ch */ 0x00, 0x28, 0x80, 0x02, 0x00, 0x04, /* 12 + total ch */ 0x00, 0x1c, H(ctr), L(ctr), 0x00, 0x00,
        /* tx channels, 2B: */ 0x00, 0x08, 0x00, 0x00, /* rx channels, 2B: */ 0x00, 0x08, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00,
        /* payload follows */ 0xc1, 0xc1, 0xc1, 0xc1,
        0xc1, 0xc1, 0xc1, 0xc1, 0xc1, 0xa1, 0xc1, 0xc1, 0xc1, 0xc1, 0xc1, 0xc1
      ]); // volume meters, 0x00 = clipping, higher values = negative dBFS, in -0.5dB steps, WHY didn't they implement that precise meters in DC?!

      // rx latency:
      if let Some(chsub) = self.channels_subscriber.as_ref() {
        let flows_info = chsub.flows_info();
        let flows_info = flows_info.read().unwrap();
        let flows_count = flows_info.len() as u16;
        bytes.write_u16(24 + flows_count * 4);
        bytes.write_u16(0x8003);
        bytes.write_u16(4);
        bytes.write_u16(12 + flows_count * 4); // content length
        bytes.write_u16(ctr);
        bytes.write_u16(0);
        bytes.write_u16(flows_count); // number of flows
        bytes.write_u16(0);
        bytes.write_u16(24);
        bytes.write_u16(0);
        bytes.write_u32(self.self_info.sample_rate);
        
        for opt in flows_info.iter() {
          let latency = opt.as_ref().map(|fi|fi.actual_latency_samples.swap(0, Ordering::Relaxed)).unwrap_or(0);
          bytes.write_u32(latency.clamp(0, i32::MAX) as u32);
        }
      }
  
      /* bytes.write_bytes(&[
        0x00, 0x1c, 0x80, 0x04,
        0x00, 0x04, 0x00, 0x10,  H(ctr), L(ctr), 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
      ]); */

      // TODO: this is response to 0738002100000064
      let required_prefix = format!("clock-stats.{}0000", hex::encode(self.self_info.mac_address.octets()));
      let mut master_clock = None;
      if let Ok(readdir) = std::fs::read_dir("/tmp") {
        for entry in readdir {
          if let Ok(entry) = entry {
            if entry.file_name().to_string_lossy().starts_with(&required_prefix) {
              if let Ok(content) = std::fs::read_to_string(entry.path()) {
                let content = content.trim_ascii();
                if content.len() >= 12 {
                  if let Ok(master_id) = hex::decode(&content[0..16]) {
                    master_clock = Some(master_id);
                    break;
                  }
                }
              }
            }
          }
        }
      }
      if let Some(mc) = master_clock {
        assert_eq!(mc.len(), 8);
        let mut clkstatus = ByteBuffer::new();
        clkstatus.set_endian(bytebuffer::Endian::BigEndian);
        clkstatus.write_bytes(&[
          0x00, 0x03, 0x00, 0x03 /* 0x01 = PLL not locked */, 0x00, 0x00, 0x00, 0x9f /* was 0xff */,
        ]);
        clkstatus.write_i32(freq_offset);
        clkstatus.write_bytes(&self.self_info.mac_address.octets());
        clkstatus.write_u16(0);
        clkstatus.write_bytes(&mc);
        clkstatus.write_bytes(&mc);
        clkstatus.write_bytes(&[0u8; 76]);
        /* clkstatus.write_bytes(&[
          0x00, 0x01, 0x00, 0x34, 0x00, 0x09, 0x00, 0x00, 0x02, 0x94, 0x00, 0x00,
          0x00, 0x03, 0x0d, 0x40, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x60, 0x0c, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x68, 0x10, 0x00,
          0x00, 0x00, 0x00, 0x01, 0x01, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x09, 0x00, 0x07,
          0x00, 0x01, 0x00, 0x02, 0x02, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x00, 0x03
        ]); */
        /* clkstatus.write_bytes(&[
          0x00, 1 /* was 1 */, 0x00, 0x34 /* was 0x34 */, 0x00, 9 /* was 9 */, 0x00, 0x00, 2 /* was 2 */, 0x34 /* was 0x34 */, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 6 /* was 6 */, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x60 /* was 0x60 */, 0x0c /* was 0xc */, 0x00,
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 1 /* was 1 */, 0x00, 0x00, 0x00, 0x68 /* was 0x68 */, 0x10 /* was 0x10 */, 0x00,
          0x00, 0x00, 0x00, 1 /* was 1 */, 1 /* was 1 */, 2 /* was 2 */, 1 /* was 1 */, 0x00, 0x00, 0x00, 0x00, 0x11 /* was 0x11 */, 0x00, 0x9 /* was 0x9 */, 0x00, 0x7 /* was 0x7 */      
        ]); */

        self.send(self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00], clkstatus.as_bytes()).await;
      }
    } else {
      debug!("no clock available");
    }

    let content = bytes.as_bytes();
    self
      .send(
        self.heartbeat_destination,
        0xfffe,
        [0, 8, 0, 1, 0x10, 0, 0, 0],
        &content,
      )
      .await;


    // this is probably response to 0738008100000064
    /* self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00],
      &[0x00, 0x18, 0x00, 0x04, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x02, 0x00, 0x00,
      // supported sample rates:
      0x00, 0x00, /* 44100: */ 0xac, 0x44, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x01, 0x58, 0x88, 0x00, 0x01, 0x77, 0x00]
    ).await; */

    // this is probably response to 0738007700000064
    self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x78, 0, 0, 0, 0],
      &[0, 0, 0, 3, 0, 0, 0, 0]
    ).await;

    self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x10, 0x07, 0, 0, 0, 0],
      &[0, 0, 0, 0]
    ).await;

    // this is probably response to 0738008300000064
    /* self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x82, 0x00, 0x00, 0x00, 0x00],
    &[
      0x00, 0x18, 0x00, 0x03, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x18, 0x00, 0x02, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x20
    ]).await; */

    /* self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x82, 0x00, 0x00, 0x00, 0x00],
      &[
        0x00, 0x18, 0x00, 0x03, 0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x18, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x18, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x20
    ]).await; */

    /* self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x10, 0x09, 0x00, 0x00, 0x00, 0x00],
      &[
        0x00, 0x00, 0x00, 0x04, 0x00, 0x02, 0x00, 0x08, 0x00, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        /* clock source: */0x00, 0x1d, 0xc1, 0xff, 0xfe, 0x11, 0x11, 0x33,
        /* transmitting to us??? : */ 0x00, 0x1d, 0xc1, 0xff, 0xfe, 0x11, 0x66, 0x33,
      ]).await; */

    // necessary, this is response to 0738002100000064
    // TODO: Clock Domain Mismatch if we send this!
    /* self.send(
      self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00],
      &[
        0x00, 0x03, 0x00, 0x03, 0x00, 0x00, 0x00, 0x9f, /* clock offset: */ 0xff, 0xff, 0x32, 0x0e,
        /* own MAC address: */ 0x00, 0x1d, 0xc1, 0xAA,
        0xBB, 0xCC, 0x00, 0x00,
        /* master MAC address: */ 0x00, 0x1d, 0xc1, 0x11, 0x11, 0x33, 0x00, 0x00,
        /* master MAC address: */ 0x00, 0x1d, 0xc1, 0x11,
        0x11, 0x33, 0x00, 0x00, 0x00, 0x01, 0x00, 0x34, 0x00, 0x09, 0x00, 0x00, 0x02, 0x94, 0x00, 0x00,
        0x00, 0x03, 0x0d, 0x40, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x60, 0x0c, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x68, 0x10, 0x00,
        0x00, 0x00, 0x00, 0x01, 0x01, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x09, 0x00, 0x07,
        0x00, 0x01, 0x00, 0x02, 0x02, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x00, 0x03
      ]
    ).await; */
 

    let mut bytes = ByteBuffer::new();
    bytes.set_endian(bytebuffer::Endian::BigEndian);
    bytes.write_bytes(&[
      0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01
    ]);
    bytes.write_bytes(&self.self_info.mac_address.octets());
    bytes.write_bytes(&self.self_info.ip_address.octets());
    bytes.write_bytes(&self.self_info.netmask.octets());
    bytes.write_bytes(&self.self_info.gateway.octets());
    bytes.write_bytes(&self.self_info.gateway.octets()); // DNS? doesn't really matter.
    bytes.write_bytes(&[
      0x00, 0x18, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    ]);
    // TODO
    // necessary, this is response to 0738001300000064:
    self.send(self.device_info_destination, 0xffff, [0x07, 0x2a, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00],
      bytes.as_bytes()
    ).await;

  }
}

pub async fn run_server(
  self_info: Arc<DeviceInfo>,
  mut rx: mpsc::Receiver<MulticastMessage>,
  clock: Arc<RwLock<MediaClock>>,
  mut channels_sub_rx: watch::Receiver<Option<Arc<ChannelsSubscriber>>>,
  shutdown: BroadcastReceiver<()>,
) {
  let server = UdpSocketWrapper::new(Some(self_info.ip_address), self_info.info_request_port, shutdown).await;
  let mut mcaster = Multicaster::new(self_info.as_ref(), server, clock);
  mcaster.send_board_info().await;
  mcaster.send_product_info().await;
  let mut heartbeat_interval = interval(Duration::from_secs(1));
  heartbeat_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
  while mcaster.should_work() {
    select! {
      r = mcaster.server.recv() => {
        let (_src, request_buf) = match r {
          Some(v) => v,
          None => continue
        };
        if request_buf.len() < 32 {
          error!("too short packet received: {}", hex::encode(request_buf));
          continue;
        }
        let opcode = &request_buf[24..32];
        match opcode {
          [0x07, _, 0, 0x61, 0, 0, 0, 0] => {
            mcaster.send_board_info().await;
          },
          [0x07, _, 0, 0xc1, 0, 0, 0, 0] => {
            mcaster.send_product_info().await;
          },
          _ => {
            warn!("unknown request to multicast port: opcode: {}", hex::encode(opcode));
            warn!("raw udp payload: {}", hex::encode(request_buf));
          }
        };
      },
      m = rx.recv() => {
        // TODO we could also make seqnum atomic and simply share socket with anyone that wants it
        if let Some(msg) = m {
          mcaster.send(mcaster.device_info_destination, msg.start_code, msg.opcode, &msg.content).await;
        } else {
          break;
        }
      },
      _ = heartbeat_interval.tick() => {
        mcaster.send_heartbeat().await;
      },
      _ = channels_sub_rx.changed() => {
        mcaster.channels_subscriber = channels_sub_rx.borrow_and_update().clone();
      }
      // TODO receive shutdown properly, currently Ctrl-C doesn't work if there is error binding to socket
    };
  }
}
