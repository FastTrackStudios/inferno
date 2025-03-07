use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;

use clock_steering::unix::UnixClock;
use clock_steering::Clock as _;
use custom_error::custom_error;
use futures::AsyncWriteExt;
use interprocess::local_socket::tokio::LocalSocketStream;
use tokio::select;
use tokio::sync::broadcast;
use futures::io::AsyncReadExt;
pub use usrvclock::ClockOverlay;
pub use usrvclock::AsyncClient as ClockReceiver;
use usrvclock::SafeClock;

use crate::{common::*, real_time_box_channel};
use crate::real_time_box_channel::RealTimeBoxReceiver;
pub type RealTimeClockReceiver = RealTimeBoxReceiver<Option<ClockOverlay>>;

/// High-precision clock (nanoseconds)
pub type FineClock = u64;

/// Signed version of the high-precision clock. For clock deltas.
pub type FineClockDiff = i64;


//#[derive(Clone)]
pub struct MediaClock {
  overlay: Option<ClockOverlay>,
  safe: SafeClock,
}

#[inline(always)]
fn timestamp_to_clock_value(ts: clock_steering::Timestamp) -> FineClock {
  (ts.seconds as FineClock).wrapping_mul(1_000_000_000).wrapping_add(ts.nanos as FineClock)
}

impl MediaClock {
  pub fn new() -> Self {
    Self {
      overlay: None,
      safe: SafeClock::new(0.01, 3_000_000_000),
    }
  }
  pub fn is_ready(&self) -> bool {
    self.overlay.is_some()
  }
  pub fn get_overlay(&self) -> &Option<ClockOverlay> {
    &self.overlay
  }
  pub fn update_overlay(&mut self, overlay: ClockOverlay) {
    /* if let Some(cur_overlay) = self.overlay {
      let cur_ovl_time = cur_overlay.now_ns();
      let new_ovl_time = overlay.now_ns();
      let diff = (new_ovl_time as ClockDiff).wrapping_sub(cur_ovl_time as ClockDiff);
      /* if diff.abs() > 10_000_000 {
        error!("clock is trying to jump dangerously by {diff} ns, ignoring update");
        return;
      } */
    } */
    self.overlay = Some(overlay);
  }
  #[inline(always)]
  pub fn now_ns(&mut self) -> Option<FineClock> {
    self.overlay.map(|overlay| {
      let safe_ts = self.safe.now(&overlay);
      if safe_ts.estimated {
        warn!("using estimated clock because of possible jump!");
      }
      safe_ts.nanos as FineClock
    })
  }
  #[inline(always)]
  pub fn now_in_timebase(&mut self, timebase_hz: u64) -> Option<Clock> {
    self.now_ns().map(|ns| {
      // TODO it will jump when underlying wraps
      ((ns as u128) * (timebase_hz as u128) / 1_000_000_000u128) as Clock
    })
  }
  pub fn system_clock_duration_until(&mut self, timestamp: Clock, timebase_hz: u64) -> Option<std::time::Duration> {
    let now_ns = self.now_ns()?;
    let to_ns = (timestamp as u128 * 1_000_000_000u128 / timebase_hz as u128) as FineClock;
    let remaining = (to_ns as FineClockDiff).wrapping_sub(now_ns as FineClockDiff);
    let corr = (remaining as f64 * self.overlay?.freq_scale) as FineClockDiff;
    let duration = remaining - corr; // FIXME it should be * 1/(1+freq_scale) but should be good enough for low correction values
    if duration > 0 {
      Some(std::time::Duration::from_nanos(duration as u64))
    } else {
      Some(std::time::Duration::from_secs(0))
    }
  }
}


pub fn start_clock_receiver(path: Option<PathBuf>) -> ClockReceiver {
  ClockReceiver::start(path.unwrap_or(usrvclock::DEFAULT_SERVER_SOCKET_PATH.into()), Box::new(|e| warn!("clock receive error: {e:?}")))
}

pub async fn make_shared_media_clock(receiver: &ClockReceiver) -> Arc<RwLock<MediaClock>> {
  let mut rx = receiver.subscribe();
  let mut media_clock = MediaClock::new();
  /* loop {
    match rx.recv().await {
      Ok(overlay) => {
        media_clock.update_overlay(overlay);
        break;
      }
      Err(broadcast::error::RecvError::Closed) => {
        panic!("ClockReceiver channel closed during initial await");
      },
      Err(e) => {
        warn!("clock receive error {e:?}");
      }
    }
  } */
  // initial await makes e.g. Audacity freeze when starting when Statime is not running. TODO figure it out
  let media_clock = Arc::new(RwLock::new(media_clock));
  let media_clock1 = media_clock.clone();
  tokio::spawn(async move {
    loop {
      let overlay_opt = rx.borrow_and_update().clone();
      if let Some(overlay) = overlay_opt {
        media_clock.write().unwrap().update_overlay(overlay);
      }
      if rx.changed().await.is_err() {
        break;
      }
    }
  });
  media_clock1
}

pub fn async_clock_receiver_to_realtime(mut receiver: tokio::sync::watch::Receiver<Option<ClockOverlay>>, initial: Option<ClockOverlay>) -> RealTimeBoxReceiver<Option<ClockOverlay>> {
  let (rt_sender, rt_recv) = real_time_box_channel::channel(Box::new(initial));
  tokio::spawn(async move {
    loop {
      let overlay_opt = receiver.borrow_and_update().clone();
      if let Some(overlay) = overlay_opt {
        rt_sender.send(Box::new(Some(overlay)));
      }
      if receiver.changed().await.is_err() {
        break;
      }
    }
  });
  rt_recv
}
