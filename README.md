# SeekableAsyncFile

This library will provide a `SeekableAsyncFile` struct that is designed to be similar in function to [tokio::fs::File](https://docs.rs/tokio/latest/tokio/fs/struct.File.html). It provides async `read_at` and `write_at` methods, which aren't currently available with Tokio.

## Getting started

Add it to your project like:

```bash
cargo add seekable-async-file
```

View the [documentation](https://docs.rs/seekable-async-file) for usage guides.

## Delayed sync

The `write_at_with_delayed_sync` method is provided to call `write` and `fdatasync`, but the sync call will be made at some future time. The returned future won't resolve until the sync call is made and completes successfully. This is a performance optimisation to try to batch multiple writes with a single sync, but still keep the semantics where the call completes only when successfully written to disk.

If this is used, make sure to also execute the `start_delayed_data_sync_background_loop` method in the background, such as using `tokio::spawn` or `tokio::join!`.

## Metrics

Metrics will be populated via the `SeekableAsyncFileMetrics` struct. All values are atomic, so it's possible to read them at any time from any thread safely using the provided getter methods. This is designed for use inside a larger system (e.g. database, object storage, cache) or I/O subsystem.

## Modes

### mmap (default)

By default, a memory map is created on the file. This means that all platforms that support mmap can be targeted, instead of only Unix platforms with `pread` and `pwrite`.

### tokio_file

Enabling the `tokio_file` feature will cause `pread` and `pwrite` on standard file descriptors to be used instead of mmap. Ensure to disable the `mmap` feature, which is enabled by default.

The target platform must support [std::os::unix::fs::FileExt](https://doc.rust-lang.org/std/os/unix/fs/trait.FileExt.html).

## Features

For experimental purposes, there are other Cargo features that can be toggled:

- **mmap**: (Default) Use mmap instead of `pread` and `pwrite`.
- **tokio_file**: Use `pread` and `pwrite` with `tokio::spawn_blocking`.
