use futures::stream::iter;
use futures::StreamExt;
use off64::usz;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::HashMap;
#[cfg(feature = "tokio_file")]
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio::time::Instant;

fn dur_us(dur: Instant) -> u64 {
  dur.elapsed().as_micros().try_into().unwrap()
}

/// Data to write and the offset to write it at. This is provided to `write_at_with_delayed_sync`.
pub struct WriteRequest {
  data: Vec<u8>,
  offset: u64,
  deduplicate_seq: Option<u64>,
}

impl WriteRequest {
  pub fn new(offset: u64, data: Vec<u8>) -> Self {
    Self {
      data,
      offset,
      deduplicate_seq: None,
    }
  }

  // POSIX write(2) calls can be reordered between fdatasync/fsync calls, so sequential writes to the same offset may not result in the last being the winner. Normally, writes to the same offset don't occur before a sync as that would be clobbering an unconfirmed change, but sometimes this is needed. Provide this variant to `write_at_with_delayed_sync` (not `write_at`), the write will be recorded (but delayed) and then applied on the next scheduled fdatasync, and the write with the highest `seq` to an offset will always win. In addition, sometimes use of async will also make it difficult to ensure ordering; therefore, a `seq` should also be provided so that any call with a less `seq` than the highest seen `seq` of all previous calls are ignored. If this is unnecessary/unwanted, always provide the same `seq` value (e.g. 0).
  pub fn deduplicated(offset: u64, seq: u64, data: Vec<u8>) -> Self {
    Self {
      data,
      offset,
      deduplicate_seq: Some(seq),
    }
  }
}

#[derive(Default)]
struct PendingDeduplicatedWrites {
  data: HashMap<u64, Vec<u8>>,
  seen_seq: HashMap<u64, u64>,
}

struct PendingSyncState {
  earliest_unsynced: Option<Instant>, // Only set when first pending_sync_fut_states is created; otherwise, metrics are misleading as we'd count time when no one is waiting for a sync as delayed sync time.
  latest_unsynced: Option<Instant>,
  pending_sync_fut_states: Vec<SignalFutureController>,
  pending_deduplicated_writes: PendingDeduplicatedWrites,
}

/// Metrics populated by a `SeekableAsyncFile`. There should be exactly one per `SeekableAsyncFile`; don't share between multiple `SeekableAsyncFile` values.
///
/// To initalise, use `SeekableAsyncFileMetrics::default()`. The values can be accessed via the thread-safe getter methods.
#[derive(Default, Debug)]
pub struct SeekableAsyncFileMetrics {
  sync_background_loops_counter: AtomicU64,
  sync_counter: AtomicU64,
  sync_delayed_counter: AtomicU64,
  sync_longest_delay_us_counter: AtomicU64,
  sync_shortest_delay_us_counter: AtomicU64,
  sync_us_counter: AtomicU64,
  write_bytes_counter: AtomicU64,
  write_counter: AtomicU64,
  write_us_counter: AtomicU64,
}

impl SeekableAsyncFileMetrics {
  /// Total number of delayed sync background loop iterations.
  pub fn sync_background_loops_counter(&self) -> u64 {
    self.sync_background_loops_counter.load(Ordering::Relaxed)
  }

  /// Total number of fsync and fdatasync syscalls.
  pub fn sync_counter(&self) -> u64 {
    self.sync_counter.load(Ordering::Relaxed)
  }

  /// Total number of requested syncs that were delayed until a later time.
  pub fn sync_delayed_counter(&self) -> u64 {
    self.sync_delayed_counter.load(Ordering::Relaxed)
  }

  /// Total number of microseconds spent waiting for a sync by one or more delayed syncs.
  pub fn sync_longest_delay_us_counter(&self) -> u64 {
    self.sync_longest_delay_us_counter.load(Ordering::Relaxed)
  }

  /// Total number of microseconds spent waiting after a final delayed sync before the actual sync.
  pub fn sync_shortest_delay_us_counter(&self) -> u64 {
    self.sync_shortest_delay_us_counter.load(Ordering::Relaxed)
  }

  /// Total number of microseconds spent in fsync and fdatasync syscalls.
  pub fn sync_us_counter(&self) -> u64 {
    self.sync_us_counter.load(Ordering::Relaxed)
  }

  /// Total number of bytes written.
  pub fn write_bytes_counter(&self) -> u64 {
    self.write_bytes_counter.load(Ordering::Relaxed)
  }

  /// Total number of write syscalls.
  pub fn write_counter(&self) -> u64 {
    self.write_counter.load(Ordering::Relaxed)
  }

  /// Total number of microseconds spent in write syscalls.
  pub fn write_us_counter(&self) -> u64 {
    self.write_us_counter.load(Ordering::Relaxed)
  }
}

/// A `File`-like value that can perform async `read_at` and `write_at` for I/O at specific offsets without mutating any state (i.e. is thread safe). Metrics are collected, and syncs can be delayed for write batching opportunities as a performance optimisation.
#[derive(Clone)]
pub struct SeekableAsyncFile {
  // Tokio has still not implemented read_at and write_at: https://github.com/tokio-rs/tokio/issues/1529. We need these to be able to share a file descriptor across threads (e.g. use from within async function).
  // Apparently spawn_blocking is how Tokio does all file operations (as not all platforms have native async I/O), so our use is not worse but not optimised for async I/O either.
  #[cfg(feature = "tokio_file")]
  fd: Arc<std::fs::File>,
  #[cfg(feature = "mmap")]
  mmap: Arc<memmap2::MmapRaw>,
  #[cfg(feature = "mmap")]
  mmap_len: usize,
  sync_delay_us: u64,
  metrics: Arc<SeekableAsyncFileMetrics>,
  pending_sync_state: Arc<Mutex<PendingSyncState>>,
}

impl SeekableAsyncFile {
  /// Open a file descriptor in read and write modes, creating it if it doesn't exist. If it already exists, the contents won't be truncated.
  ///
  /// If the mmap feature is being used, to save a `stat` call, the size must be provided. This also allows opening non-standard files which may have a size of zero (e.g. block devices). A different size value also allows only using a portion of the beginning of the file.
  ///
  /// The `io_direct` and `io_dsync` parameters set the `O_DIRECT` and `O_DSYNC` flags, respectively. Unless you need those flags, provide `false`.
  ///
  /// Make sure to execute `start_delayed_data_sync_background_loop` in the background after this call.
  pub async fn open(
    path: &Path,
    #[cfg(feature = "mmap")] size: u64,
    metrics: Arc<SeekableAsyncFileMetrics>,
    sync_delay: Duration,
    flags: i32,
  ) -> Self {
    let async_fd = OpenOptions::new()
      .read(true)
      .write(true)
      .custom_flags(flags)
      .open(path)
      .await
      .unwrap();

    let fd = async_fd.into_std().await;

    SeekableAsyncFile {
      #[cfg(feature = "tokio_file")]
      fd: Arc::new(fd),
      #[cfg(feature = "mmap")]
      mmap: Arc::new(memmap2::MmapRaw::map_raw(&fd).unwrap()),
      #[cfg(feature = "mmap")]
      mmap_len: usz!(size),
      sync_delay_us: sync_delay.as_micros().try_into().unwrap(),
      metrics,
      pending_sync_state: Arc::new(Mutex::new(PendingSyncState {
        earliest_unsynced: None,
        latest_unsynced: None,
        pending_sync_fut_states: Vec::new(),
        pending_deduplicated_writes: PendingDeduplicatedWrites::default(),
      })),
    }
  }

  // Since spawn_blocking requires 'static lifetime, we don't have a read_into_at function taht takes a &mut [u8] buffer, as it would be more like a Arc<Mutex<Vec<u8>>>, at which point the overhead is not really worth it for small reads.
  #[cfg(feature = "tokio_file")]
  pub async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    let fd = self.fd.clone();
    spawn_blocking(move || {
      let mut buf = vec![0u8; len.try_into().unwrap()];
      fd.read_exact_at(&mut buf, offset).unwrap();
      buf
    })
    .await
    .unwrap()
  }

  #[cfg(feature = "mmap")]
  pub unsafe fn get_mmap_raw_ptr(&self, offset: u64) -> *const u8 {
    self.mmap.as_ptr().add(usz!(offset))
  }

  #[cfg(feature = "mmap")]
  pub unsafe fn get_mmap_raw_mut_ptr(&self, offset: u64) -> *mut u8 {
    self.mmap.as_mut_ptr().add(usz!(offset))
  }

  #[cfg(feature = "mmap")]
  pub async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    let offset = usz!(offset);
    let len = usz!(len);
    let mmap = self.mmap.clone();
    let mmap_len = self.mmap_len;
    spawn_blocking(move || {
      let memory = unsafe { std::slice::from_raw_parts(mmap.as_ptr(), mmap_len) };
      memory[offset..offset + len].to_vec()
    })
    .await
    .unwrap()
  }

  #[cfg(feature = "mmap")]
  pub fn read_at_sync(&self, offset: u64, len: u64) -> Vec<u8> {
    let offset = usz!(offset);
    let len = usz!(len);
    let memory = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr(), self.mmap_len) };
    memory[offset..offset + len].to_vec()
  }

  fn bump_write_metrics(&self, len: u64, call_us: u64) {
    self
      .metrics
      .write_bytes_counter
      .fetch_add(len, Ordering::Relaxed);
    self.metrics.write_counter.fetch_add(1, Ordering::Relaxed);
    self
      .metrics
      .write_us_counter
      .fetch_add(call_us, Ordering::Relaxed);
  }

  #[cfg(feature = "tokio_file")]
  pub async fn write_at(&self, offset: u64, data: Vec<u8>) {
    let fd = self.fd.clone();
    let len: u64 = data.len().try_into().unwrap();
    let started = Instant::now();
    spawn_blocking(move || fd.write_all_at(&data, offset).unwrap())
      .await
      .unwrap();
    // Yes, we're including the overhead of Tokio's spawn_blocking.
    let call_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.bump_write_metrics(len, call_us);
  }

  #[cfg(feature = "mmap")]
  pub async fn write_at(&self, offset: u64, data: Vec<u8>) {
    let offset = usz!(offset);
    let len = data.len();
    let started = Instant::now();

    let mmap = self.mmap.clone();
    let mmap_len = self.mmap_len;
    spawn_blocking(move || {
      let memory = unsafe { std::slice::from_raw_parts_mut(mmap.as_mut_ptr(), mmap_len) };
      memory[offset..offset + len].copy_from_slice(&data);
    })
    .await
    .unwrap();

    // This could be significant e.g. page fault.
    let call_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.bump_write_metrics(len.try_into().unwrap(), call_us);
  }

  #[cfg(feature = "mmap")]
  pub fn write_at_sync(&self, offset: u64, data: Vec<u8>) {
    let offset = usz!(offset);
    let len = data.len();
    let started = Instant::now();

    let memory = unsafe { std::slice::from_raw_parts_mut(self.mmap.as_mut_ptr(), self.mmap_len) };
    memory[offset..offset + len].copy_from_slice(&data);

    // This could be significant e.g. page fault.
    let call_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.bump_write_metrics(len.try_into().unwrap(), call_us);
  }

  pub async fn write_at_with_delayed_sync(&self, writes: Vec<WriteRequest>) {
    let count: u64 = writes.len().try_into().unwrap();
    let mut deduplicated = vec![];
    for w in writes {
      match w.deduplicate_seq {
        Some(_) => deduplicated.push(w),
        // TODO Can we make this concurrent?
        None => self.write_at(w.offset, w.data).await,
      };
    }

    let (fut, fut_ctl) = SignalFuture::new();

    {
      let mut state = self.pending_sync_state.lock().await;
      let now = Instant::now();
      state.earliest_unsynced.get_or_insert(now);
      state.latest_unsynced = Some(now);
      state.pending_sync_fut_states.push(fut_ctl.clone());

      for w in deduplicated {
        if state
          .pending_deduplicated_writes
          .seen_seq
          .get(&w.offset)
          .filter(|s| **s > w.deduplicate_seq.unwrap())
          .is_none()
        {
          state
            .pending_deduplicated_writes
            .data
            .insert(w.offset, w.data);
          state
            .pending_deduplicated_writes
            .seen_seq
            .insert(w.offset, w.deduplicate_seq.unwrap());
        };
      }
    };

    self
      .metrics
      .sync_delayed_counter
      .fetch_add(count, Ordering::Relaxed);

    fut.await;
  }

  pub async fn start_delayed_data_sync_background_loop(&self) {
    // Store these outside and reuse them to avoid reallocations on each loop.
    let mut futures_to_wake = Vec::new();
    let mut deduplicated_writes = Vec::new();
    loop {
      sleep(std::time::Duration::from_micros(self.sync_delay_us)).await;

      struct SyncNow {
        longest_delay_us: u64,
        shortest_delay_us: u64,
      }

      let sync_now = {
        let mut state = self.pending_sync_state.lock().await;

        if !state.pending_sync_fut_states.is_empty()
          || !state.pending_deduplicated_writes.data.is_empty()
        {
          let longest_delay_us = dur_us(state.earliest_unsynced.unwrap());
          let shortest_delay_us = dur_us(state.latest_unsynced.unwrap());

          state.earliest_unsynced = None;
          state.latest_unsynced = None;

          futures_to_wake.extend(state.pending_sync_fut_states.drain(..));
          deduplicated_writes.extend(state.pending_deduplicated_writes.data.drain());

          Some(SyncNow {
            longest_delay_us,
            shortest_delay_us,
          })
        } else {
          None
        }
      };

      if let Some(SyncNow {
        longest_delay_us,
        shortest_delay_us,
      }) = sync_now
      {
        // OPTIMISATION: Don't perform these atomic operations while unnecessarily holding up the lock.
        self
          .metrics
          .sync_longest_delay_us_counter
          .fetch_add(longest_delay_us, Ordering::Relaxed);
        self
          .metrics
          .sync_shortest_delay_us_counter
          .fetch_add(shortest_delay_us, Ordering::Relaxed);

        iter(deduplicated_writes.drain(..))
          .for_each_concurrent(None, |(offset, data)| async move {
            self.write_at(offset, data).await;
          })
          .await;

        self.sync_data().await;

        for ft in futures_to_wake.drain(..) {
          ft.signal();
        }
      };

      self
        .metrics
        .sync_background_loops_counter
        .fetch_add(1, Ordering::Relaxed);
    }
  }

  pub async fn sync_data(&self) {
    #[cfg(feature = "tokio_file")]
    let fd = self.fd.clone();
    #[cfg(feature = "mmap")]
    let mmap = self.mmap.clone();

    let started = Instant::now();
    spawn_blocking(move || {
      #[cfg(feature = "tokio_file")]
      fd.sync_data().unwrap();

      #[cfg(feature = "mmap")]
      mmap.flush().unwrap();
    })
    .await
    .unwrap();
    // Yes, we're including the overhead of Tokio's spawn_blocking.
    let sync_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.metrics.sync_counter.fetch_add(1, Ordering::Relaxed);
    self
      .metrics
      .sync_us_counter
      .fetch_add(sync_us, Ordering::Relaxed);
  }
}
