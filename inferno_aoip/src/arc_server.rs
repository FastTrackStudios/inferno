use crate::channels_subscriber::ChannelsSubscriber;
use crate::{byte_utils::*, net_utils};

use crate::device_info::DeviceInfo;
use crate::flows_control_server::FlowInfo as TXFlowInfo;
use crate::flows_rx::MAX_FLOWS as MAX_RX_FLOWS;
use crate::flows_tx::MAX_FLOWS as MAX_TX_FLOWS;
use crate::info_mcast_server::MulticastMessage;
use crate::mdns_server::DeviceMDNSResponder;
use crate::net_utils::UdpSocketWrapper;
use crate::protocol::mcast::make_channel_change_notification;
use crate::protocol::req_resp::{self, CODE_OK};
use crate::protocol::req_resp::HEADER_LENGTH;
use crate::protocol::proto_arc::*;
use crate::state_storage::{SavedChannelsSettings, StateStorage};
use crate::utils::LogAndForget;
use binary_serde::recursive_array::RecursiveArray as _;
use binary_serde::BinarySerde;
use bytebuffer::{ByteBuffer, Endian};
use itertools::Itertools;
use log::{error, info, trace};
use std::sync::RwLock;
use std::{cmp::min, sync::Arc};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;

pub async fn run_server(
  self_info: Arc<DeviceInfo>,
  state_storage: Arc<StateStorage>,
  mdns_server: Arc<DeviceMDNSResponder>,
  mcast: Sender<MulticastMessage>,
  mut channels_sub_rx: watch::Receiver<Option<Arc<ChannelsSubscriber>>>,
  tx_flows_info: Arc<RwLock<Vec<Option<TXFlowInfo>>>>,
  shutdown: BroadcastReceiver<()>,
) {
  let mut subscriber = None;
  let mut saved_channels = SavedChannelsSettings::load(state_storage, self_info.clone());
  for (index, _) in self_info.tx_channels.iter().enumerate() {
    mdns_server.add_tx_channel(index);
  }
  let server = UdpSocketWrapper::new(Some(self_info.ip_address), self_info.arc_port, shutdown).await;
  let mut conn = req_resp::Connection::new(server);
  let mut recv_buff = net_utils::ReceiveBuffer::new();
  while conn.should_work() {
    let request = match conn.recv(&mut recv_buff).await {
      Some(v) => v,
      None => continue,
    };

    if channels_sub_rx.has_changed().unwrap_or(false) {
      subscriber = channels_sub_rx.borrow_and_update().clone();
    }

    if request.opcode2().read() == 0 {
      match request.opcode1().read() {

        channels_and_flows_count::OPCODE => {
          let total_channels_wtf = self_info.tx_channels.len() + self_info.rx_channels.len(); // ??? not actually total number of channels but in some devices it is
          let response = channels_and_flows_count::Response {
            unknown1_0: 0,
            flags2: channels_and_flows_count::Flags2 { supports_tx_channel_rename: true, ..Default::default() },
            tx_channels_count: self_info.tx_channels.len().try_into().unwrap(),
            rx_channels_count: self_info.rx_channels.len().try_into().unwrap(),
            unknown2_4: 4, // or 1
            unknown3_4: 4, // or 8
            unknown4_8: 8,
            max_tx_flows: MAX_TX_FLOWS.try_into().unwrap(),
            max_rx_flows: MAX_RX_FLOWS.try_into().unwrap(),
            unknown5_total_channels: total_channels_wtf.try_into().unwrap(),
            unknown6_1: 1,
            unknown7_1: 1,
            unknown8_0: [0; 6],
          };
          conn.respond_with_struct(CODE_OK, response).await;
        }

        GET_DEVICE_NAME_OPCODE => {
          // device name (used by network-audio-controller)
          let mut buff = ByteBuffer::new();
          buff.write_bytes(self_info.friendly_hostname.as_bytes());
          buff.write_u8(0);
          conn.respond(buff.as_bytes()).await;
        }

        get_device_names::OPCODE => {
          let mut bytes = ByteBuffer::new();
          let strings_offset = HEADER_LENGTH;
          bytes.write_bytes(&[0u8; get_device_names::ResponseHeader::SERIALIZED_SIZE]);
          let friendly_hostname_offset = (bytes.get_wpos() + strings_offset).try_into().unwrap();
          bytes.write_bytes(self_info.friendly_hostname.as_bytes());
          bytes.write_u8(0);
          let factory_hostname_offset = (bytes.get_wpos() + strings_offset).try_into().unwrap();
          bytes.write_bytes(self_info.factory_hostname.as_bytes());
          bytes.write_u8(0);
          let board_name_offset = (bytes.get_wpos() + strings_offset).try_into().unwrap();
          bytes.write_bytes(self_info.board_name.as_bytes());
          bytes.write_u8(0);
          let revision_string_offset = (bytes.get_wpos() + strings_offset).try_into().unwrap();
          bytes.write_bytes(b":705\0");

          let response = get_device_names::ResponseHeader {
            board_name_offset,
            revision_string_offset,
            friendly_hostname_offset1: friendly_hostname_offset,
            factory_hostname_offset,
            friendly_hostname_offset2: friendly_hostname_offset,
            start_code: 0x2729,
            unknown_opcode_1102: 0x1102,
            ..Default::default()
          };
          bytes.set_wpos(0);
          bytes.write_bytes(response.binary_serialize_to_array(binary_serde::Endianness::Big).as_slice());
          conn.respond(&bytes.as_bytes()).await;
        }

        get_receive_channels::OPCODE => {
          // Dante Receivers names and subscriptions:

          let mut common_descriptor_offset: u16 = 0;
          paginate_respond(
            &mut conn,
            request.content(),
            if subscriber.is_some() { self_info.rx_channels.len().min(32).try_into().unwrap() } else { 0 },
            self_info.rx_channels.iter().enumerate(),
            |(channel_index, ch), bytes| {
              if common_descriptor_offset == 0 {
                let descr = CommonChannelsDescriptor::new(&self_info);
                common_descriptor_offset = bytes.get_wpos().try_into().unwrap();
                bytes.write_bytes(descr.binary_serialize_to_array(binary_serde::Endianness::Big).as_slice());
              }
              let status = subscriber.as_ref().unwrap().channel_status(channel_index);
              let (tx_channel_name_offset, tx_hostname_offset) = match &status {
                None => (0, 0),
                Some(status) => (
                  write_0term_str_to_bytebuffer(bytes, &status.tx_channel_name),
                  write_0term_str_to_bytebuffer(bytes, &status.tx_hostname),
                )
              };
              let status_value: u32 = match &status {
                None => 0,
                Some(ss) => ss.status as u32,
              };
              get_receive_channels::ChannelDescriptor {
                channel_id: (channel_index + 1).try_into().unwrap(),
                unknown1_6: 6,
                common_descriptor_offset,
                tx_channel_name_offset,
                tx_hostname_offset,
                friendly_name_offset: write_0term_str_to_bytebuffer(bytes, &ch.friendly_name.read().unwrap()),
                subscription_status: status_value,
                unknown2_0: 0,
              }
            }
          ).await;
        }

        get_transmit_channels::OPCODE => {
          // Dante Transmitters default names:
          let mut common_descriptor_offset: u16 = 0;
          paginate_respond(
            &mut conn,
            request.content(),
            self_info.tx_channels.len().min(32).try_into().unwrap(),
            self_info.tx_channels.iter().enumerate(),
            |(channel_index, ch), bytes| {
              if common_descriptor_offset == 0 {
                let descr = CommonChannelsDescriptor::new(&self_info);
                common_descriptor_offset = bytes.get_wpos().try_into().unwrap();
                bytes.write_bytes(descr.binary_serialize_to_array(binary_serde::Endianness::Big).as_slice());
              }
              get_transmit_channels::ChannelDescriptor {
                channel_id: (channel_index + 1).try_into().unwrap(),
                unknown1_7: 7,
                common_descriptor_offset,
                name_offset: write_0term_str_to_bytebuffer(bytes, &ch.factory_name),
              }
            }
          ).await;
        }

        get_transmit_channels_friendly_names::OPCODE => {
          // Dante Transmitters user-specified names:
          let mut wrote = false;
          paginate_respond(
            &mut conn,
            request.content(),
            self_info.tx_channels.len().min(32).try_into().unwrap(),
            self_info.tx_channels.iter().enumerate(),
            |(channel_index, ch), bytes| {
              if !wrote {
                bytes.write_u32(0);
                wrote = true;
              }
              let channel_id = (channel_index + 1).try_into().unwrap();
              get_transmit_channels_friendly_names::ChannelDescriptor {
                channel_id_1: channel_id,
                channel_id_2: channel_id,
                friendly_name_offset: write_0term_str_to_bytebuffer(bytes, &ch.friendly_name.read().unwrap()),
              }
            }
          ).await;
        }

        rename_tx_channel::OPCODE => {
          // rename TX channel
          let content = request.content();
          let mut renamed_ids = deserialize_items::<rename_tx_channel::SingleChannelRenameRequest>(content).filter_map(|rename| {
            let channel_id = rename.channel_id.saturating_sub(HEADER_LENGTH as _);
            let name_offset = rename.new_name_offset.saturating_sub(HEADER_LENGTH as _);
            if channel_id==0 || name_offset==0 {
              return None;
            }
            match read_0term_str_from_buffer(content, name_offset as usize - HEADER_LENGTH) {
              Ok(new_name) => {
                let index = (channel_id - 1) as usize;
                if index < self_info.tx_channels.len() {
                  info!("renaming TX channel id {channel_id} to {new_name}");
                  mdns_server.remove_tx_channel(index);
                  saved_channels.rename_tx_channel(index, new_name.to_owned());
                  mdns_server.add_tx_channel(index);
                  Some(channel_id)
                } else {
                  error!("got rename TX channel request with invalid channel number {channel_id}");
                  None
                }
              }
              Err(e) => {
                error!("could not read new channel name from packet: {e:?}");
                None
              }
            }
          });
          let renamed_anything = renamed_ids.next().is_some();
          renamed_ids.for_each(drop); // consume the whole iterator

          if renamed_anything {
            conn.respond_with_code(0 /* TODO: really? */, &[]).await;
          } else {
            conn.respond_with_code(1, &[0, 0]).await;
            // sometimes it is [0, 1, 0, 0, H(channel_id), L(channel_id)], but it doesn't look necessary
          }
        }
        0x3001 => {
          // rename RX channel
          // TODO support multiple renames in request
          let content = request.content();
          let channel_id = make_u16(content[2], content[3]);
          let name_offset = make_u16(content[4], content[5]);
          let mut channel_indices = vec![];
          let code = match read_0term_str_from_buffer(content, name_offset as usize - HEADER_LENGTH) {
            Ok(new_name) => {
              let index = (channel_id - 1) as usize;
              if index < self_info.rx_channels.len() {
                info!("renaming RX channel id {channel_id} to {new_name}");
                channel_indices.push(index);
                saved_channels.rename_rx_channel(index, new_name.to_owned());
                1
              } else {
                error!("got rename RX channel request with invalid channel number {channel_id}");
                0 // really?
              }
            }
            Err(e) => {
              error!("could not read new channel name from packet: {e:?}");
              0 // really?
            }
          };
          conn.respond_with_code(code, &[]).await;
          if code == 1 {
            mcast.send(make_channel_change_notification(channel_indices)).await.log_and_forget();
          }
        }
        0x2200 => {
          // query TX flows
          let content = request.content();
          let start_index = make_u16(content[2], content[3]) as usize - 1;
          let mut response = ByteBuffer::new();
          let code = {
            let flows_info = tx_flows_info.read().unwrap();
            let remaining = flows_info.iter().skip(start_index).filter(|opt| opt.is_some()).count();
            let limit = 12;
            let in_this_response = min(limit, remaining);

            response.write_u8(in_this_response as u8);
            response.write_u8(in_this_response as u8);
            for _ in 0..in_this_response {
              response.write_u16(0);
            }
            let mut flow_positions = vec![];

            for (flow_index, flow_info_opt) in
              flows_info.iter().enumerate().skip(start_index).take(in_this_response)
            {
              // 58 bytes per descriptor (with 2 channels and 2-word mask)
              // 46 + channels_per_flow * (bytes_per_mask + 2)
              if flow_info_opt.is_none() {
                continue;
              }
              let flow_info = flow_info_opt.as_ref().unwrap();

              let flow_id = flow_index + 1;
              let local_tx_flow_name_offset = response.get_wpos() + HEADER_LENGTH;
              let flow_name = format!("{}_{}", flow_id, self_info.process_id);
              response.write_bytes(flow_name.as_bytes());
              response.write_u8(0);

              let remote_hostname_offset = response.get_wpos() + HEADER_LENGTH;
              response.write_bytes(flow_info.rx_hostname.as_bytes());
              response.write_u8(0);

              let remote_rx_flow_name_offset = response.get_wpos() + HEADER_LENGTH;
              response.write_bytes(flow_info.rx_flow_name.as_bytes());
              response.write_u8(0);

              while ((response.get_wpos() + HEADER_LENGTH) % 4) != 0 {
                response.write_u8(0);
              }
              let descriptor1_pos = response.get_wpos() + HEADER_LENGTH;
              response.write_u16(0x0802);
              response.write_u16(flow_info.rx_port);
              response.write_bytes(&flow_info.rx_addr.octets());

              let descriptor2_pos = response.get_wpos();
              response.write_bytes(&[0x0a, 0x00, 0x00, 0x01]);
              response.write_u16(remote_hostname_offset as u16);
              response.write_u16(remote_rx_flow_name_offset as u16);
              response.write_u16(0x10); // ??? 0x3c (60), or 0x10...
              response.write_u16(local_tx_flow_name_offset as u16);
              response.write_bytes(&[0u8; 8]);

              flow_positions.push(response.get_wpos() + HEADER_LENGTH);
              response.write_u16(flow_id.try_into().unwrap());
              response.write_u16(0x11); // or 2 for multicast
              response.write_u32(self_info.sample_rate);
              response.write_u16(0);
              response.write_u16(0x18);
              response.write_u16(1);
              response.write_u16(flow_info.local_channel_indices.len().try_into().unwrap());
              response.write_u16(descriptor1_pos as u16);
              for ch in &flow_info.local_channel_indices {
                response.write_u16(ch.map(|i| i + 1).unwrap_or(0).try_into().unwrap());
              }
              response.write_u16(descriptor2_pos as u16);
            }

            response.set_wpos(2);
            for pos in flow_positions {
              response.write_u16(pos as _);
            }
            if remaining > in_this_response {
              0x8112
            } else {
              1
            }
          };
          conn.respond_with_code(code, response.as_bytes()).await;
        }
        0x2320 => {
          // ???
          conn.respond_with_code(0x30, &[]).await;
        }

        0x2201 => {
          // Create multicast TX flow
        }
        0x3200 => {
          // query RX flows
          let mut response = ByteBuffer::new();
          response.set_endian(Endian::BigEndian);
          let code = if let Some(chsub) = subscriber.as_ref() {
            let flows_info = chsub.flows_info();
            let flows_info = flows_info.read().unwrap();
            let content = request.content();
            let start_index = make_u16(content[2], content[3]) as usize - 1;
            let remaining = flows_info.iter().skip(start_index).filter(|opt| opt.is_some()).count();
            let limit = 12;
            let in_this_response = min(limit, remaining);

            response.write_u8(in_this_response as _);
            response.write_u8(in_this_response as _);
            for _ in 0..in_this_response {
              response.write_u16(0);
            }
            let mut flow_positions = vec![];
            for (flow_index, flow_info_opt) in
              flows_info.iter().enumerate().skip(start_index).take(in_this_response)
            {
              // 58 bytes per descriptor (with 2 channels and 2-word mask)
              // 46 + channels_per_flow * (bytes_per_mask + 2)
              if flow_info_opt.is_none() {
                continue;
              }
              while ((response.get_wpos() + HEADER_LENGTH) % 4) != 0 {
                response.write_u16(0);
              }
              let flow_info = flow_info_opt.as_ref().unwrap();

              let descriptor1_pos = response.get_wpos() + HEADER_LENGTH;
              response.write_u16(0x0802);
              response.write_u16(flow_info.rx_port);
              response.write_bytes(&self_info.ip_address.octets());

              let descriptor2_pos = response.get_wpos() + HEADER_LENGTH;
              response.write_bytes(&[0x00, 0x09, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00]);
              response.write_u32(
                (flow_info.latency_samples as u64 * 1_000_000_000u64 / self_info.sample_rate as u64)
                  .try_into()
                  .unwrap(),
              );
              response.write_u32(0);

              let words_per_bitmask = 2;
              let bitmasks_start = response.get_wpos() + HEADER_LENGTH;
              for mask in &flow_info.channels_map {
                let mut chi = 0;
                for _ in 0..words_per_bitmask {
                  let mut word: u16 = 0;
                  let mut single_bit = 1;
                  while single_bit != 0 {
                    word |= if mask.get(chi).unwrap_or(false) { single_bit } else { 0 };
                    chi += 1;
                    single_bit <<= 1;
                  }
                  response.write_u16(word);
                }
              }

              while ((response.get_wpos() + HEADER_LENGTH) % 4) != 0 {
                response.write_u16(0);
              }
              flow_positions.push(response.get_wpos() + HEADER_LENGTH);
              response.write_u16((flow_index + 1) as _);
              response.write_u16(1);
              response.write_u32(self_info.sample_rate);
              response.write_bytes(&[0x00, 0x00, 0x00, 0x18, 0x00, 0x01]);
              response.write_u16(flow_info.channels_map.len() as _); // was 2
              response.write_u16(words_per_bitmask as _); // 2-byte-words per bitmask, was 1

              response.write_u16(descriptor1_pos as _);
              for i in 0..flow_info.channels_map.len() {
                response.write_u16((bitmasks_start + i * 2 * words_per_bitmask) as _);
              }
              response.write_u16(descriptor2_pos as _);
            }

            response.set_wpos(2);
            for pos in flow_positions {
              response.write_u16(pos as _);
            }
            if remaining > in_this_response {
              0x8112
            } else {
              1
            }
          } else {
            response.write_bytes(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
            1
          };
          conn.respond_with_code(code, response.as_bytes()).await;
        }

        0x1100 => {
          // used by DC
          // received unknown opcode1 0x1100, content 00130201820482050210021182188219830183028306031003110303802100f08060002200630064
          // whole packet: "272900320e621100000000130201820482050210021182188219830183028306031003110303802100f08060002200630064"

          // ???
          // looks like something dependent on active connections
          let content = [0u8; 110];
          // XXX: not necessary
          /* let content = [
            0x12, 0x12, 0x02, 0x01, 0x00, 0x01, 0x82, 0x04, 0x00, 0x54, 0x82, 0x05, 0x00, 0x58,
            0x02, 0x10, 0x00, 0x10, 0x02, 0x11, 0x00, 0x10, 0x00, 0x00, 0x82, 0x18, 0x00, 0x00,
            0x82, 0x19, 0x83, 0x01, 0x00, 0x5c, 0x83, 0x02, 0x00, 0x60, 0x83, 0x06, 0x00, 0x64,
            0x03, 0x10, 0x00, 0x10, 0x03, 0x11, 0x00, 0x10, 0x03, 0x03, 0x00, 0x02, 0x80, 0x21,
            0x00, 0x68, 0x00, 0x00, 0x00, 0xf0, 0x00, 0x00, 0x80, 0x60, 0x00, 0x22, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x63, /* 1000000: */ 0x00, 0x0f, 0x42, 0x40, 0x00, 0x0f, 0x42,
            0x40, 0x00, 0x0f, 0x42, 0x40, 0x01, 0x35, 0xf1, 0xb4, 0x00, 0x0f, 0x42, 0x40, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00,
          ]; */
          conn.respond(&content).await;
        }
        0x1102 => {
          // identical for all low channels count devices
          let content = [0u8; 94];
          // XXX not necessary
          /* let content = [
            /* number of items, 2B: */ 0x00, 0x17, 0x80, 0x20, 0x00, 0x01,
            0x80, 0x21, 0x00, 0x03, 0x00, 0x22, 0x00, 0x03, 0x00, 0x23, 0x00, 0x03, 0x00, 0x24, 0x00, 0x01,
            0x02, 0x01, 0x00, 0x03, 0x82, 0x04, 0x00, 0x03, 0x82, 0x05, 0x00, 0x03, 0x02, 0x0a, 0x00, 0x01,
            0x02, 0x0b, 0x00, 0x01, 0x02, 0x10, 0x00, 0x03, 0x02, 0x11, 0x00, 0x03, 0x02, 0x12, 0x00, 0x03,
            0x02, 0x13, 0x00, 0x01, 0x02, 0x14, 0x00, 0x01, 0x83, 0x01, 0x00, 0x03, 0x83, 0x06, 0x00, 0x01,
            0x83, 0x02, 0x00, 0x01, 0x03, 0x10, 0x00, 0x01, 0x03, 0x11, 0x00, 0x01, 0x03, 0x03, 0x00, 0x03,
            0x83, 0xf0, 0x00, 0x01, 0x06, 0x01, 0x00, 0x01
          ]; */
          conn.respond(&content).await;
        }
        0x3300 => {
          // WTF: this is necessary to avoid 'clock domain mismatch' error in DC
          conn.respond(&[0x38, 0x00, 0x38, 0xfd, 0x38, 0xfe, 0x38, 0xff]).await;
          //conn.respond(&[0u8; 8]).await;
        }

        0x3010 => {
          // subscribe (connect our receiver to remote transmitter)
          // or unsubscribe if tx_*_offset is 0
          if let Some(channels_recv) = &subscriber {
            let c_whole = request.content();
            let count = c_whole[1] as usize;
            for i in 0..count {
              let c = &c_whole[2 + i * 6..];
              let local_channel = make_u16(c[0], c[1]);
              let tx_channel_offset = make_u16(c[2], c[3]) as usize;
              let tx_hostname_offset = make_u16(c[4], c[5]) as usize;
              let local_channel_index = (local_channel - 1) as usize;
              if tx_channel_offset > 0 && tx_hostname_offset > 0 {
                let str_or_none = |offset| match offset {
                  _ if offset < HEADER_LENGTH => None,
                  v => match read_0term_str_from_buffer(&c_whole, v - HEADER_LENGTH) {
                    Ok(s) => Some(s),
                    Err(e) => {
                      error!("failed to decode string: {e:?}");
                      None
                    }
                  },
                };
                let tx_channel_name = str_or_none(tx_channel_offset);
                let tx_hostname = str_or_none(tx_hostname_offset);
                info!(
                  "connection requested: {} <- {:?} @ {:?}",
                  local_channel, tx_channel_name, tx_hostname
                );
                if tx_channel_name.is_some() && tx_hostname.is_some() {
                  channels_recv
                    .subscribe(local_channel_index, tx_channel_name.unwrap(), tx_hostname.unwrap())
                    .await;
                } else {
                  error!("couldn't read tx names from subscription request: {}", hex::encode(&c_whole));
                }
              } else {
                info!("disconnect requested: local channel {}", local_channel);
                channels_recv.unsubscribe(local_channel_index).await;
              }
            }
            conn.respond(&[]).await;
          }
        }

        0x3014 => {
          // netaudio subscription remove
          // received unknown opcode1 0x3014, content 000100000002
          // whole packet: "27ff00104a1c30140000000100000002"
          if let Some(channels_recv) = &subscriber {
            let content = request.content();
            let local_channel = make_u16(content[4], content[5]);
            let local_channel_index = (local_channel - 1) as usize;
            info!("disconnect requested: local channel {}", local_channel);
            channels_recv.unsubscribe(local_channel_index).await;
            conn.respond(&[]).await;
          }
        }

        x => {
          error!("received unknown opcode1 {x:#04x}, content {}", hex::encode(request.content()));
          error!("whole packet: {:?}", hex::encode(request.into_storage()));
        }
      }
    } else {
      error!(
        "received unknown opcode2 {:#04x}, content {}",
        request.opcode2().read(),
        hex::encode(request.content())
      );
      error!("whole packet: {:?}", hex::encode(request.into_storage()));
    }
  }
}
