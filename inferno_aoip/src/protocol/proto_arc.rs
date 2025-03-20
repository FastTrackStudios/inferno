

pub const PORT: u16 = 4440;

pub mod channels_and_flows_count {
  use binrw::{binrw, BinRead, BinWrite};
  use modular_bitfield::prelude::*;
  
  pub const OPCODE: u16 = 0x1000;

  #[bitfield]
  #[derive(BinRead, BinWrite, Clone, Copy)]
  #[br(map = Self::from_bytes)]
  #[bw(map = |&x| Self::into_bytes(x))]
  pub struct Flags1 {
    pub unknown1: B3,
    pub supports_tx_channel_rename: bool,
    pub unknown2: B4,
  }

  #[binrw]
  #[brw(big)]
  pub struct Response {
    pub unknown1_0: u8, // or 5
    pub flags1: Flags1,
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
