use async_trait::async_trait;
use off64::chrono::Off64AsyncReadChrono;
use off64::chrono::Off64AsyncWriteChrono;
use off64::chrono::Off64ReadChrono;
use off64::chrono::Off64WriteChrono;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64AsyncWriteInt;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteInt;
use off64::usz;
use off64::Off64AsyncRead;
use off64::Off64AsyncWrite;
use off64::Off64Read;
use off64::Off64Write;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::io::SeekFrom;
#[cfg(feature = "tokio_file")]
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tinybuf::TinyBuf;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io;
use tokio::io::AsyncSeekExt;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio::time::Instant;

pub async fn get_file_len_via_seek(path: &Path) -> io::Result<u64> {
  let mut file = File::open(path).await?;
  // Note that `file.metadata().len()` is 0 for device files.
  file.seek(SeekFrom::End(0)).await
}

fn dur_us(dur: Instant) -> u64 {
  dur.elapsed().as_micros().try_into().unwrap()
}

/// Data to write and the offset to write it at. This is provided to `write_at_with_delayed_sync`.
pub struct WriteRequest<D: AsRef<[u8]> + Send + 'static> {
  data: D,
  offset: u64,
}

impl<D: AsRef<[u8]> + Send + 'static> WriteRequest<D> {
  pub fn new(offset: u64, data: D) -> Self {
    Self { data, offset }
  }
}

struct PendingSyncState {
  earliest_unsynced: Option<Instant>, // Only set when first pending_sync_fut_states is created; otherwise, metrics are misleading as we'd count time when no one is waiting for a sync as delayed sync time.
  latest_unsynced: Option<Instant>,
  pending_sync_fut_states: Vec<SignalFutureController>,
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
      })),
    }
  }

  #[cfg(feature = "mmap")]
  pub unsafe fn get_mmap_raw_ptr(&self, offset: u64) -> *const u8 {
    self.mmap.as_ptr().add(usz!(offset))
  }

  #[cfg(feature = "mmap")]
  pub unsafe fn get_mmap_raw_mut_ptr(&self, offset: u64) -> *mut u8 {
    self.mmap.as_mut_ptr().add(usz!(offset))
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

  #[cfg(feature = "mmap")]
  pub async fn read_at(&self, offset: u64, len: u64) -> TinyBuf {
    let offset = usz!(offset);
    let len = usz!(len);
    let mmap = self.mmap.clone();
    let mmap_len = self.mmap_len;
    spawn_blocking(move || {
      let memory = unsafe { std::slice::from_raw_parts(mmap.as_ptr(), mmap_len) };
      TinyBuf::from_slice(&memory[offset..offset + len])
    })
    .await
    .unwrap()
  }

  #[cfg(feature = "mmap")]
  pub fn read_at_sync(&self, offset: u64, len: u64) -> TinyBuf {
    let offset = usz!(offset);
    let len = usz!(len);
    let memory = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr(), self.mmap_len) };
    TinyBuf::from_slice(&memory[offset..offset + len])
  }

  #[cfg(feature = "tokio_file")]
  pub async fn read_at(&self, offset: u64, len: u64) -> TinyBuf {
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
  pub async fn write_at<D: AsRef<[u8]> + Send + 'static>(&self, offset: u64, data: D) {
    let offset = usz!(offset);
    let len = data.as_ref().len();
    let started = Instant::now();

    let mmap = self.mmap.clone();
    let mmap_len = self.mmap_len;
    spawn_blocking(move || {
      let memory = unsafe { std::slice::from_raw_parts_mut(mmap.as_mut_ptr(), mmap_len) };
      memory[offset..offset + len].copy_from_slice(data.as_ref());
    })
    .await
    .unwrap();

    // This could be significant e.g. page fault.
    let call_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.bump_write_metrics(len.try_into().unwrap(), call_us);
  }

  #[cfg(feature = "mmap")]
  pub fn write_at_sync<D: AsRef<[u8]> + Send + 'static>(&self, offset: u64, data: D) -> () {
    let offset = usz!(offset);
    let len = data.as_ref().len();
    let started = Instant::now();

    let memory = unsafe { std::slice::from_raw_parts_mut(self.mmap.as_mut_ptr(), self.mmap_len) };
    memory[offset..offset + len].copy_from_slice(data.as_ref());

    // This could be significant e.g. page fault.
    let call_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.bump_write_metrics(len.try_into().unwrap(), call_us);
  }

  #[cfg(feature = "tokio_file")]
  pub async fn write_at<D: AsRef<[u8]> + Send + 'static>(&self, offset: u64, data: D) {
    let fd = self.fd.clone();
    let len: u64 = data.as_ref().len().try_into().unwrap();
    let started = Instant::now();
    spawn_blocking(move || fd.write_all_at(data.as_ref(), offset).unwrap())
      .await
      .unwrap();
    // Yes, we're including the overhead of Tokio's spawn_blocking.
    let call_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.bump_write_metrics(len, call_us);
  }

  pub async fn write_at_with_delayed_sync<D: AsRef<[u8]> + Send + 'static>(
    &self,
    writes: impl IntoIterator<Item = WriteRequest<D>>,
  ) {
    let mut count: u64 = 0;
    // WARNING: If we're using mmap, we cannot make this concurrent as that would make writes out of order, and caller may require writes to clobber each other sequentially, not nondeterministically.
    // TODO For File, we could write concurrently, as writes are not guaranteed to be ordered anyway. However, caller may still want to depend on platform-specific guarantees of ordered writes if available.
    for w in writes {
      count += 1;
      self.write_at(w.offset, w.data).await;
    }

    let (fut, fut_ctl) = SignalFuture::new();

    {
      let mut state = self.pending_sync_state.lock().await;
      let now = Instant::now();
      state.earliest_unsynced.get_or_insert(now);
      state.latest_unsynced = Some(now);
      state.pending_sync_fut_states.push(fut_ctl.clone());
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
    loop {
      sleep(std::time::Duration::from_micros(self.sync_delay_us)).await;

      struct SyncNow {
        longest_delay_us: u64,
        shortest_delay_us: u64,
      }

      let sync_now = {
        let mut state = self.pending_sync_state.lock().await;

        if !state.pending_sync_fut_states.is_empty() {
          let longest_delay_us = dur_us(state.earliest_unsynced.unwrap());
          let shortest_delay_us = dur_us(state.latest_unsynced.unwrap());

          state.earliest_unsynced = None;
          state.latest_unsynced = None;

          futures_to_wake.extend(state.pending_sync_fut_states.drain(..));

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

#[cfg(feature = "mmap")]
impl<'a> Off64Read<'a, TinyBuf> for SeekableAsyncFile {
  fn read_at(&'a self, offset: u64, len: u64) -> TinyBuf {
    self.read_at_sync(offset, len)
  }
}
#[cfg(feature = "mmap")]
impl<'a> Off64ReadChrono<'a, TinyBuf> for SeekableAsyncFile {}
#[cfg(feature = "mmap")]
impl<'a> Off64ReadInt<'a, TinyBuf> for SeekableAsyncFile {}

#[cfg(feature = "mmap")]
impl Off64Write for SeekableAsyncFile {
  fn write_at(&self, offset: u64, value: &[u8]) -> () {
    self.write_at_sync(offset, TinyBuf::from_slice(value))
  }
}
#[cfg(feature = "mmap")]
impl Off64WriteChrono for SeekableAsyncFile {}
#[cfg(feature = "mmap")]
impl Off64WriteInt for SeekableAsyncFile {}

#[async_trait]
impl<'a> Off64AsyncRead<'a, TinyBuf> for SeekableAsyncFile {
  async fn read_at(&self, offset: u64, len: u64) -> TinyBuf {
    SeekableAsyncFile::read_at(self, offset, len).await
  }
}
impl<'a> Off64AsyncReadChrono<'a, TinyBuf> for SeekableAsyncFile {}
impl<'a> Off64AsyncReadInt<'a, TinyBuf> for SeekableAsyncFile {}

#[async_trait]
impl Off64AsyncWrite for SeekableAsyncFile {
  async fn write_at(&self, offset: u64, value: &[u8]) {
    SeekableAsyncFile::write_at(self, offset, TinyBuf::from_slice(value)).await
  }
}
impl Off64AsyncWriteChrono for SeekableAsyncFile {}
impl Off64AsyncWriteInt for SeekableAsyncFile {}
