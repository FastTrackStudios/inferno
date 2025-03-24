use binary_serde::BinarySerde;
use bytebuffer::ByteBuffer;
use log::error;

use crate::byte_utils::make_u16;

use super::req_resp::{Connection, HEADER_LENGTH};


pub const PACKET_SIZE_SOFT_LIMIT: usize = 800;
pub const PORT: u16 = 4440;

pub mod channels_and_flows_count {
  use binary_serde::{binary_serde_bitfield, BinarySerde, BitfieldBitOrder};

  pub const OPCODE: u16 = 0x1000;

  #[derive(Debug, Default, PartialEq, Eq)]
  #[binary_serde_bitfield(order = BitfieldBitOrder::LsbFirst)]
  pub struct Flags2 {
    #[bits(4)]
    pub unknown1_0: u8,
    #[bits(1)]
    pub supports_tx_channel_rename: bool,
    #[bits(3)]
    pub unknown2_0: u8,
  }

  #[derive(Debug, BinarySerde, Default, PartialEq, Eq)]
  pub struct Response {
    pub unknown1_0: u8, // or 5
    pub flags2: Flags2,
    pub tx_channels_count: u16,
    pub rx_channels_count: u16,
    pub unknown2_4: u16, // or 1
    pub unknown3_4: u16, // or 8
    pub unknown4_8: u16,
    pub max_tx_flows: u16,
    pub max_rx_flows: u16,
    pub unknown5_total_channels: u16,
    pub unknown6_1: u16,
    pub unknown7_1: u16,
    pub unknown8_0: [u16; 6],
  }
}

pub const GET_DEVICE_NAME_OPCODE: u16 = 0x1002;

pub mod get_device_names {
  use binary_serde::BinarySerde;

  pub const OPCODE: u16 = 0x1003;

  #[derive(Debug, BinarySerde, Default)]
  pub struct ResponseHeader {
    pub unknown1_0: u16,
    pub unknown2_0: u16, // was 0x14
    pub unknown3_0: u16, // was 0x20
    pub board_name_offset: u16,
    pub revision_string_offset: u16,
    pub unknown4_0: u16, // was 0x500
    pub friendly_hostname_offset1: u16,
    pub factory_hostname_offset: u16,
    pub friendly_hostname_offset2: u16,
    pub unknown5_0: [u16; 6], // was [0, 0, 4, 0, 4, 0]
    pub start_code: u16, // 0x2729
    pub unknown6_0: u16,
    pub unknown_opcode_1102: u16,
    pub unknown7_0: u16,
  }
}

pub mod get_receive_channels {
  use binary_serde::BinarySerde;

  pub const OPCODE: u16 = 0x3000;

  #[derive(Debug, BinarySerde, Default)]
  pub struct ChannelDescriptor {
    pub channel_id: u16,
    pub unknown1_6: u16,
    pub common_descriptor_offset: u16,
    pub tx_channel_name_offset: u16,
    pub tx_hostname_offset: u16,
    pub friendly_name_offset: u16,
    pub subscription_status: u32, // TODO. 0x01010009 if subscribed currently, 0x00000001 if not found but remembers subscription or in progress
    pub unknown2_0: u32,
  }

  #[derive(Debug, BinarySerde, Default)]
  pub struct CommonDescriptor {
    pub sample_rate: u32,
    pub unknown1_1: u8,
    pub unknown2_1: u8,
    pub bits_per_sample_1: u16,
    pub unknown3_400: u16,
    pub bits_per_sample_2: u16,
    pub bits_per_sample_3: u16,
    pub pcm_type: u16,
  }
}

pub fn serialize_items<InItem, OutItem>(
  space_items: u8,
  source: impl IntoIterator<Item = InItem>,
  mut transform: impl FnMut(InItem, &mut ByteBuffer) -> OutItem
) -> (bool, Vec<u8>)
  where OutItem: BinarySerde {
  let mut bytes = ByteBuffer::new();
  bytes.write_bytes(&[0u8; HEADER_LENGTH]);
  bytes.write_u8(space_items);
  bytes.write_u8(0);
  if space_items == 0 {
    return (false, bytes.as_bytes()[HEADER_LENGTH..].into());
  }
  let space_items: usize = space_items.into();
  bytes.write_bytes(&vec![0u8; space_items*OutItem::SERIALIZED_SIZE]);
  
  let source = source.into_iter();
  let mut item_pos = 2 + HEADER_LENGTH;
  let mut actual_items = 0;
  let mut have_more = false;

  let mut tmp_buffer = vec![0u8; OutItem::SERIALIZED_SIZE];
  for in_item in source {
    if actual_items >= space_items {
      have_more = true;
      break;
    }
    let out_item = transform(in_item, &mut bytes);
    out_item.binary_serialize(&mut tmp_buffer, binary_serde::Endianness::Big);
    let prev_pos = bytes.get_wpos();
    bytes.set_wpos(item_pos);
    bytes.write_bytes(&tmp_buffer);
    bytes.set_wpos(prev_pos);
    item_pos += OutItem::SERIALIZED_SIZE;
    if prev_pos >= PACKET_SIZE_SOFT_LIMIT {
      have_more = true;
      break;
    }
    actual_items += 1;
  }
  bytes.set_wpos(1 + HEADER_LENGTH);
  bytes.write_u8(actual_items.try_into().unwrap());
  (have_more, bytes.as_bytes()[HEADER_LENGTH..].into())
}

pub fn extract_start_index(request_payload: &[u8]) -> Option<usize> {
  if request_payload.len() < 4 || (request_payload[2]|request_payload[3])==0 {
    error!("got invalid paginate request, payload: {request_payload:?}");
    return None;
  }
  Some((make_u16(request_payload[2], request_payload[3]) - 1).into())
}

pub async fn paginate_respond<InItem, OutItem>(
  connection: &mut Connection,
  request_payload: &[u8],
  space_items: u8,
  source: impl IntoIterator<Item = InItem>,
  transform: impl FnMut(InItem, &mut ByteBuffer) -> OutItem
)
  where OutItem: BinarySerde {
  let start_index = match extract_start_index(request_payload) {
    Some(v) => v,
    None => return
  };
  let (have_more, bytes) = serialize_items(space_items, source.into_iter().skip(start_index), transform);
  let code = if have_more { 0x8112 } else { 1 };
  connection.respond_with_code(code, &bytes).await;
}
