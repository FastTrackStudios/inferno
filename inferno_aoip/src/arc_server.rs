use crate::byte_utils::*;
use crate::channels_subscriber::ChannelsSubscriber;

use crate::device_info::DeviceInfo;
use crate::info_mcast_server::MulticastMessage;
use crate::net_utils::UdpSocketWrapper;
use crate::protocol::mcast::make_channel_change_notification;
use crate::protocol::req_resp;
use crate::protocol::req_resp::HEADER_LENGTH;
use crate::flows_rx::MAX_FLOWS as MAX_RX_FLOWS;
use crate::flows_tx::MAX_FLOWS as MAX_TX_FLOWS;
use crate::flows_control_server::FlowInfo as TXFlowInfo;
use crate::state_storage::{SavedChannelsSettings, StateStorage};
use crate::utils::LogAndForget;
use bytebuffer::{ByteBuffer, Endian};
use log::{error, info, trace, warn};
use tokio::sync::mpsc::Sender;
use std::sync::RwLock;
use std::{cmp::min, sync::Arc};
use tokio::sync::broadcast::Receiver as BroadcastReceiver;
use tokio::sync::watch;


pub async fn run_server(
  self_info: Arc<DeviceInfo>,
  state_storage: Arc<StateStorage>,
  mcast: Sender<MulticastMessage>,
  mut channels_sub_rx: watch::Receiver<Option<Arc<ChannelsSubscriber>>>,
  tx_flows_info: Arc<RwLock<Vec<Option<TXFlowInfo>>>>,
  shutdown: BroadcastReceiver<()>,
) {
  let mut subscriber = None;
  let mut saved_channels = SavedChannelsSettings::load(state_storage, self_info.clone());
  let server = UdpSocketWrapper::new(Some(self_info.ip_address), self_info.arc_port, shutdown).await;
  let mut conn = req_resp::Connection::new(server);
  while conn.should_work() {
    let request = match conn.recv().await {
      Some(v) => v,
      None => continue,
    };

    if channels_sub_rx.has_changed().unwrap_or(false) {
      subscriber = channels_sub_rx.borrow_and_update().clone();
    }

    if request.opcode2().read() == 0 {
      match request.opcode1().read() {
        0x1000 => {
          let txnum = self_info.tx_channels.len() as u16;
          let rxnum = self_info.rx_channels.len() as u16;
          let total_channels_wtf = txnum + rxnum; // ??? not actually total number of channels but in some devices it is
          conn
            .respond(&[
              0, // was 0x05 but then no channel and flows are shown
              0x10, // was 0xf9, 0x10 = supports TX channel renames
              H(txnum),
              L(txnum),
              H(rxnum),
              L(rxnum),
              0x00,
              0x04, // was 4, or 1...
              0x00,
              0x04, // was 4, or 8...
              0x00,
              0x08, // was 8
              H(MAX_TX_FLOWS as _),
              L(MAX_TX_FLOWS as _),
              H(MAX_RX_FLOWS as _), // max receive flows MSB
              L(MAX_RX_FLOWS as _), // max receive flows LSB
              H(total_channels_wtf as _),
              L(total_channels_wtf as _), // was 4, 0 also occurs in some devices
              0x00,
              0x01, // was 1
              0x00,
              0x01, // if 0, DC doesn't recognize RX channels
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
            ])
            .await;
        }
        0x1002 => {
          // device name (used by network-audio-controller)
          let mut buff = ByteBuffer::new();
          buff.write_bytes(self_info.friendly_hostname.as_bytes());
          buff.write_u8(0);
          conn.respond(buff.as_bytes()).await;
        }
        0x1003 => {
          // hostname, board name, factory names:
          let mut content = [
            0x00, 0x00, 0x00, 0 /*was 0x14*/, 0x00, 0 /*was 0x20*/, /* offset of board name: */ 0x00, 0x70,
            /* offset of :70{2,5} (revision string?) */ 0x00, 0x90, 0 /*was 5*/, 0x00,
            /* offset of friendly host name: */ 0x00, 0x30,
            /* offset of factory host name: */ 0x00, 0x50,
            /* offset of friendly host name again: */ 0x00, 0x30, 0x00, 0x00, 0x00, 0x00,
            0 /*was 4*/, 0x00, 0x00, 0x00, 0 /*was 4*/, 0x00, 0 /*was 2*/, /* for :705, 1 for :702 */
            0x00, /* start code: */ 0x27, 0x29, 0 /*was 2*/, 0 /*was 4*/,
            /* opcode of some other request: */ 0x11, 0x02, 0 /*was 0x10*/, 0 /*was 4*/,
            /* friendly host name: */
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, /* factory host name: */
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, /* board name: */
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, /* :70{2,5} */
            0x3a, 0x37, 0x30, 0x35, 0x00,
          ];
          let HOST_NAMES_MAXLEN = 0x20 - 1;
          let BOARD_NAME_MAXLEN = 23;
          write_str_to_buffer(
            &mut content,
            0x30 - HEADER_LENGTH,
            HOST_NAMES_MAXLEN,
            &self_info.friendly_hostname,
          );
          write_str_to_buffer(
            &mut content,
            0x50 - HEADER_LENGTH,
            HOST_NAMES_MAXLEN,
            &self_info.factory_hostname,
          );
          write_str_to_buffer(
            &mut content,
            0x70 - HEADER_LENGTH,
            BOARD_NAME_MAXLEN,
            &self_info.board_name,
          );
          conn.respond(&content).await;
        }

        0x3000 => {
          // Dante Receivers names and subscriptions:
          let content = request.content();
          let start_index = make_u16(content[2], content[3]) - 1;
          let remaining = if subscriber.is_some() { self_info.rx_channels.len().saturating_sub(start_index as usize) } else { 0 };
          let limit = 20; // TODO
          let in_this_response = min(limit, remaining);
          trace!("returning {in_this_response} rx channels starting with index {start_index}");
          let mut response = ByteBuffer::new();
          response.set_endian(Endian::BigEndian);
          response.write_u8(in_this_response as u8);
          response.write_u8(in_this_response as u8); // number of channels in this response(?)
          let common_descr_offset = HEADER_LENGTH + 2 + in_this_response * 20;
          let strings_offset = common_descr_offset + 16;
          let mut strings = ByteBuffer::new();
          for (i, ch) in self_info
            .rx_channels
            .iter()
            .enumerate()
            .skip(start_index as usize)
            .take(in_this_response)
          {
            response.write_u16((i + 1) as u16); // channel id
            response.write_u16(6); // ???
            response.write_u16(common_descr_offset as u16);
            let status = subscriber.as_ref().unwrap().channel_status(i);
            let (tx_ch_offset, tx_host_offset) = match &status {
              None => (0, 0),
              Some(status) => {
                let tx_ch_offset = (strings.get_wpos() + strings_offset) as u16;
                strings.write_bytes(status.tx_channel_name.as_bytes());
                strings.write_u8(0);
                let tx_host_offset = (strings.get_wpos() + strings_offset) as u16;
                strings.write_bytes(status.tx_hostname.as_bytes());
                strings.write_u8(0);
                (tx_ch_offset, tx_host_offset)
              }
            };
            response.write_u16(tx_ch_offset);
            response.write_u16(tx_host_offset);
            response.write_u16((strings.get_wpos() + strings_offset) as u16);
            strings.write_bytes(ch.friendly_name.read().unwrap().as_bytes());
            strings.write_u8(0);
            let status_value: u32 = match &status {
              None => 0,
              Some(ss) => ss.status as u32,
            };
            response.write_u32(status_value); // subscription status, TODO. 0x01010009 if subscribed currently, 0x00000001 if not found but remembers subscription or in progress
            response.write_u32(0); // ???
          }
          response.write_u32(self_info.sample_rate);
          response.write_bytes(&[
            0x01,
            0x01,
            0x00,
            0x18,
            0x04,
            0x00,
            0x00,
            0x18,
            0x00,
            0x18,
            0x00,
            self_info.pcm_type,
          ]);
          response.write_bytes(strings.as_bytes());
          let code = if remaining > in_this_response { 0x8112 } else { 1 };
          conn.respond_with_code(code, response.as_bytes()).await;
        }

        0x2000 => {
          // Dante Transmitters default names:
          let content = request.content();
          let start_index = make_u16(content[2], content[3]) as usize - 1;
          let remaining = self_info.tx_channels.len() - start_index as usize;
          let limit = 16;
          let in_this_response = min(limit, remaining);
          trace!("returning {in_this_response} tx channels default names starting with index {start_index}");
          
          let channels_names_total: usize =
            self_info.tx_channels.iter().skip(start_index).take(in_this_response).map(|ch| ch.factory_name.len() + 1).sum();
          let mut content = vec![0; 2 + in_this_response * 8 + 16 + channels_names_total];
          content[0] = in_this_response as u8;
          content[1] = in_this_response as u8;
          let mut ch_descr_offset: u16 = HEADER_LENGTH as u16 + 2;
          let common_descr_offset: u16 = ch_descr_offset + in_this_response as u16 * 8;

          let mut descr = ByteBuffer::new();
          descr.write_u32(self_info.sample_rate);
          descr.write_bytes(&[
            0x01,
            0x01,
            0x00,
            0x18,
            0x04,
            0x00,
            0x00,
            0x18,
            0x00,
            0x18,
            0,
            self_info.pcm_type,
          ]);
          content[common_descr_offset as usize - HEADER_LENGTH as usize..][..16]
            .clone_from_slice(descr.as_bytes());
          let mut name_offset: u16 = common_descr_offset + 16;
          for (i, ch) in self_info
            .tx_channels
            .iter()
            .enumerate()
            .skip(start_index as usize)
            .take(in_this_response) {
            let channel_id = (i + 1) as u16;
            content[ch_descr_offset as usize - HEADER_LENGTH..][..8].clone_from_slice(&[
              H(channel_id),
              L(channel_id),
              0,
              7,
              H(common_descr_offset),
              L(common_descr_offset),
              H(name_offset),
              L(name_offset),
            ]);
            write_str_to_buffer(
              &mut content,
              (name_offset as usize) - HEADER_LENGTH,
              ch.factory_name.len(),
              &ch.factory_name,
            );
            ch_descr_offset += 8;
            name_offset += ch.factory_name.len() as u16 + 1;
          }
          let code = if remaining > in_this_response { 0x8112 } else { 1 };
          conn.respond_with_code(code, &content).await;
          // TODO rewrite this with ByteBuffer
        }
        0x2010 => {
          // Dante Transmitters user-specified names:
          /*respond(&[0x04, 0x04, 0x00, 0x01, 0x00, 0x01, 0x00, 0x2c, 0x00, 0x02, 0x00, 0x02, 0x00, 0x38, 0x00, 0x03, 0x00, 0x03, 0x00, 0x44, 0x00, 0x04, 0x00, 0x04, 0x00, 0x4d, 0x00, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46, 0x4f, 0x64, 0x62, 0x69, 0x6f, 0x72, 0x6e, 0x69, 0x6b, 0x2d, 0x4c, 0x00, 0x4f, 0x64, 0x62, 0x69, 0x6f, 0x72, 0x6e, 0x69, 0x6b, 0x2d, 0x52, 0x00, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x63, 0x6f, 0x6d, 0x00, 0x34, 0x00]);*/
          let content = request.content();
          let start_index = make_u16(content[2], content[3]) as usize - 1;
          let remaining = self_info.tx_channels.len() - start_index as usize;
          let limit = 16;
          let in_this_response = min(limit, remaining);
          trace!("returning {in_this_response} tx channels friendly names starting with index {start_index}");

          let mut response = ByteBuffer::new();
          response.write_u8(in_this_response as u8);
          response.write_u8(in_this_response as u8);
          let mut strings = ByteBuffer::new();
          let strings_offset = HEADER_LENGTH + 2 + in_this_response * 6 + 4;
          for (i, ch) in self_info
            .tx_channels
            .iter()
            .enumerate()
            .skip(start_index as usize)
            .take(in_this_response) {
            let channel_id = (i + 1) as u16;
            response.write_u16(channel_id);
            response.write_u16(channel_id);
            response.write_u16((strings.get_wpos() + strings_offset) as u16);
            strings.write_bytes(ch.friendly_name.read().unwrap().as_bytes());
            strings.write_u8(0);
          }
          response.write_u32(0); // ??? used to be 0,0,0,1, or 1,1,0,9, maybe random memory fragments???
          response.write_bytes(strings.as_bytes());

          let code = if remaining > in_this_response { 0x8112 } else { 1 };
          conn.respond_with_code(code, response.as_bytes()).await;
        }
        0x2013 => { // rename TX channel
          // TODO support multiple renames in request
          let content = request.content();
          let channel_id = make_u16(content[4], content[5]);
          let name_offset = make_u16(content[6], content[7]);
          let mut response = ByteBuffer::new();
          let code = match read_0term_str_from_buffer(content, name_offset as usize - HEADER_LENGTH) {
            Ok(new_name) => {
              let index = (channel_id-1) as usize;
              if index < self_info.tx_channels.len() {
                info!("renaming TX channel id {channel_id} to {new_name}");
                saved_channels.rename_tx_channel(index, new_name.to_owned());
                response.write_bytes(&[0, 1, 0, 0]);
                response.write_u16(channel_id);
                1
              } else {
                error!("got rename TX channel request with invalid channel number {channel_id}");
                0 // TODO
              }
            }
            Err(e) => {
              error!("could not read new channel name from packet: {e:?}");
              0 // TODO
            }
          };
          conn.respond_with_code(code, response.as_bytes()).await;
        }
        0x3001 => { // rename RX channel
          // TODO support multiple renames in request
          let content = request.content();
          let channel_id = make_u16(content[2], content[3]);
          let name_offset = make_u16(content[4], content[5]);
          let mut channel_indices = vec![];
          let code = match read_0term_str_from_buffer(content, name_offset as usize - HEADER_LENGTH) {
            Ok(new_name) => {
              let index = (channel_id-1) as usize;
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
          if code==1 {
            mcast
              .send(make_channel_change_notification(channel_indices))
              .await.log_and_forget();
          }
        }
        0x2200 => { // query TX flows
          let content = request.content();
          let start_index = make_u16(content[2], content[3]) as usize - 1;
          let mut response = ByteBuffer::new();
          let code = {
            let flows_info = tx_flows_info.read().unwrap();
            let remaining = flows_info.iter().skip(start_index).filter(|opt|opt.is_some()).count();
            let limit = 12;
            let in_this_response = min(limit, remaining);

            response.write_u8(in_this_response as u8);
            response.write_u8(in_this_response as u8);
            for _ in 0..in_this_response {
              response.write_u16(0);
            }
            let mut flow_positions = vec![];

            for (flow_index, flow_info_opt) in flows_info.iter().enumerate().skip(start_index).take(in_this_response) {
              // 58 bytes per descriptor (with 2 channels and 2-word mask)
              // 46 + channels_per_flow * (bytes_per_mask + 2)
              if flow_info_opt.is_none() { continue; }
              let flow_info = flow_info_opt.as_ref().unwrap();
              
              let flow_id = flow_index+1;
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
                response.write_u16(ch.map(|i|i+1).unwrap_or(0).try_into().unwrap());
              }
              response.write_u16(descriptor2_pos as u16);
            }

            response.set_wpos(2);
            for pos in flow_positions {
              response.write_u16(pos as _);
            }
            if remaining > in_this_response { 0x8112 } else { 1 }
          };
          conn.respond_with_code(code, response.as_bytes()).await;
        }
        0x2320 => { // ???
          conn.respond_with_code(0x30, &[]).await;
        }

        0x2201 => {
          // Create multicast TX flow
        }
        0x3200 => { // query RX flows
          let mut response = ByteBuffer::new();
          response.set_endian(Endian::BigEndian);
          let code = if let Some(chsub) = subscriber.as_ref() {
            let flows_info = chsub.flows_info();
            let flows_info = flows_info.read().unwrap();
            let content = request.content();
            let start_index = make_u16(content[2], content[3]) as usize - 1;
            let remaining = flows_info.iter().skip(start_index).filter(|opt|opt.is_some()).count();
            let limit = 12;
            let in_this_response = min(limit, remaining);
            
            response.write_u8(in_this_response as _);
            response.write_u8(in_this_response as _);
            for _ in 0..in_this_response {
              response.write_u16(0);
            }
            let mut flow_positions = vec![];
            for (flow_index, flow_info_opt) in flows_info.iter().enumerate().skip(start_index).take(in_this_response) {
              // 58 bytes per descriptor (with 2 channels and 2-word mask)
              // 46 + channels_per_flow * (bytes_per_mask + 2)
              if flow_info_opt.is_none() { continue; }
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
              response.write_u32((flow_info.latency_samples as u64 * 1_000_000_000u64 / self_info.sample_rate as u64).try_into().unwrap());
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
                response.write_u16((bitmasks_start + i*2*words_per_bitmask) as _);
              }
              response.write_u16(descriptor2_pos as _);
            }

            response.set_wpos(2);
            for pos in flow_positions {
              response.write_u16(pos as _);
            }
            if remaining > in_this_response { 0x8112 } else { 1 }
          } else {
            response.write_bytes(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
            1
          };
          conn.respond_with_code(code, response.as_bytes()).await;
        }

        0x1100 => { // used by DC
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
          conn
            .respond(&content)
            .await;
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
          conn
            .respond(&content)
            .await;
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
                  error!(
                    "couldn't read tx names from subscription request: {}",
                    hex::encode(&c_whole)
                  );
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
