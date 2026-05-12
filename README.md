# rget

A fast, multi-connection HTTP file downloader written in Rust. Designed to fully saturate high-speed internet connections, making it ideal for downloading large files like AI models.

## Features

- Multi-connection downloads to maximize bandwidth utilization
- Per-chunk and overall progress bars (10 Hz redraw cap to keep terminal flicker low)
- The slowest active chunk is highlighted in red so you can see at a glance which connection is dragging the download down; finished chunks turn green
- Supervisor that detects and restarts hung or persistently-lagging connections (capped at one restart per chunk to avoid loops)
- Optional aggressive supervisor mode for more eager restarts once a finished majority is reached
- Automatic resume of cancelled chunks via HTTP `Range` (no re-downloading already-written bytes)
- Strict 206-Partial-Content checking so a server that ignores `Range` and returns the full body can never silently corrupt the output
- IPv4 / IPv6 forcing flags
- Optional SHA-256 verification at the end (via system `sha256sum`)
- Interactive overwrite prompt by default, with explicit `--overwrite` / `--no-overwrite` flags for non-interactive use
- HEAD-then-ranged-GET probe so signed URLs (e.g. S3 presigned GETs) work without an extra round trip

## Installation

### From source

Ensure you have Rust installed. Then:

```bash
git clone <repository-url>
cd rget
cargo build --release
```

The binary will be at `target/release/rget`.

### From cargo

```bash
cargo install rget
```

## Usage

```bash
rget [OPTIONS] <URL>
```

### Options

| Flag | Description |
| --- | --- |
| `-o, --output <FILE>` | Output file path. Default: inferred from `Content-Disposition` then URL path, falling back to `download.bin`. |
| `-n, --connections <N>` | Number of parallel connections. Default: `8`. |
| `--min-chunk <BYTES>` | Minimum chunk size per connection, in bytes. Default: `1048576` (1 MiB). |
| `-4, --ipv4` | Force IPv4 for the connection (analogous to `ping -4`). Mutually exclusive with `-6`. |
| `-6, --ipv6` | Force IPv6 for the connection (analogous to `ping -6`). |
| `--aggressive` | Aggressive supervisor mode: once more than half of the connections have finished, restart any active connection still below 50 % completion. The default supervisor only restarts a chunk when at most 2 connections remain active. |
| `--overwrite` | Overwrite an existing output file without prompting. Mutually exclusive with `--no-overwrite`. |
| `--no-overwrite` | Refuse to overwrite an existing output file (exit cleanly instead of prompting). |
| `--no-sha256` | Skip the SHA-256 verification step after the download completes. |
| `-h, --help` | Print help. |

If neither `--overwrite` nor `--no-overwrite` is set and the output file already exists, `rget` will prompt `Overwrite? [Y/n]` on a TTY. Running with a non-TTY stdin (e.g. from a script) without one of those flags is an error rather than a silent default, so you don't accidentally clobber files in CI.

### Examples

Default 8-connection download:

```bash
rget https://example.com/large-file.zip
```

16 connections, force IPv4, custom output path:

```bash
rget -n 16 -4 -o ./out/large-file.zip https://example.com/large-file.zip
```

Aggressive supervisor and skip the SHA-256 step (useful for very large files where the verification doubles the wall-clock time):

```bash
rget --aggressive --no-sha256 -n 16 https://example.com/model.safetensors
```

Scripted download, overwrite without prompting:

```bash
rget --overwrite -o /var/cache/blob.bin https://example.com/blob.bin
```

## How the supervisor decides to restart a chunk

A separate "supervisor" task wakes every 500 ms and inspects each active chunk's progress. It will cancel and re-issue a single chunk's HTTP request (resuming from the bytes already written) under any of these conditions:

- **Hung connection** (always, regardless of mode): the chunk has transferred fewer than 64 KiB in the last 15 s.
- **Lagging chunk in default mode**: at most 2 connections are still active, at least one has finished, the laggard is below 50 % completion, and the lag has been sustained for at least 10 s.
- **Lagging chunk in `--aggressive` mode**: at least half of all connections have finished, the laggard is below 50 % completion, and the lag has been sustained for at least 5 s.

In every case a chunk may only be restarted once. After a restart there is a 15 s cooldown before the chunk can be re-evaluated. These guarantees prevent the supervisor from getting stuck in a restart loop on a fundamentally slow link.

## License

MIT
