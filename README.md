# rget

A fast, multi-connection HTTP file downloader written in Rust. Designed to fully saturate high-speed internet connections, making it ideal for downloading large files like AI models.

## Features

- Multi-connection downloads to maximize bandwidth utilization
- Per-chunk and overall progress bars (10 Hz redraw cap to keep terminal flicker low)
- The slowest active chunk is highlighted in red so you can see at a glance which connection is dragging the download down; finished chunks turn green
- Supervisor that detects and restarts hung or persistently-lagging connections (capped at one restart per chunk to avoid loops)
- Optional aggressive supervisor mode for more eager restarts once a finished majority is reached
- Cross-run resume support using hidden control files (`.filename.rget`) — automatically continues interrupted downloads across runs
- Automatic resume of cancelled chunks via HTTP `Range` (intra-run)
- Strict 206-Partial-Content checking so a server that ignores `Range` and returns the full body can never silently corrupt the output
- IPv4 / IPv6 forcing flags
- SHA-256 / SHA-512 verification (CLI flags or automatic sidecar `.sha256`/`.sha512` files)
- Resource protection: `--limit-rate` to cap bandwidth and `--max-size` to prevent disk exhaustion from malicious `Content-Length` values
- Interactive overwrite prompt by default, with explicit `--overwrite` / `--no-overwrite` flags for non-interactive use
- HEAD-then-ranged-GET probe so signed URLs (e.g. S3 presigned GETs) work without an extra round trip
- Batch downloads via multiple URLs or `-i` file, with `--fail-fast`
- URL range expansion for sharded files (e.g. `model-{001..040}-of-00040.safetensors`)
- Optional HTTP/3 (QUIC) support for better performance on high-latency/lossy networks (requires `--features http3`)

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
| `--min-chunk <SIZE>` | Minimum chunk size per connection (e.g. 1M, 256K, 1048576). Default: 1M. |
| `-4, --ipv4` | Force IPv4 for the connection (analogous to `ping -4`). Mutually exclusive with `-6`. |
| `-6, --ipv6` | Force IPv6 for the connection (analogous to `ping -6`). |
| `--aggressive` | Aggressive supervisor mode: once more than half of the connections have finished, restart any active connection still below 50 % completion. The default supervisor only restarts a chunk when at most 2 connections remain active. |
| `--overwrite` | Overwrite an existing output file without prompting. Mutually exclusive with `--no-overwrite`. |
| `--no-overwrite` | Refuse to overwrite an existing output file (exit cleanly instead of prompting). |
| `--sha256 <HEX>` | Verify the download against the given SHA-256 checksum. Fails the run on mismatch. Sidecar files (`<file>.sha256`) are detected automatically. |
| `--sha512 <HEX>` | Verify the download against the given SHA-512 checksum. Fails the run on mismatch. Sidecar files (`<file>.sha512`) are detected automatically. |
| `--no-sha` | Skip all checksum computation and verification for this run. |
| `--no-continue` | Disable cross-run resume support entirely. No resume control file will be read or written. |
| `--limit-rate <SPEED>` | Limit the overall download speed (e.g. 50M, 2G, 500K). The limit applies across all connections combined. |
| `--max-size <SIZE>` | Maximum allowed file size (e.g. 500G, 2T, 100M). Protects against malicious servers returning huge `Content-Length` values that could exhaust disk space. Default: 2T. |
| `-i, --input-file <FILE>` | Read URLs from a file (one per line). Lines starting with `#` are ignored. Can be combined with positional URLs. |
| `--fail-fast` | When processing multiple URLs, stop immediately on the first failure. |
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

Aggressive supervisor and skip checksum verification (useful for very large files where the verification doubles the wall-clock time):

```bash
rget --aggressive --no-sha -n 16 https://example.com/model.safetensors
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

## Cross-run Resume Support

`rget` supports resuming downloads across multiple invocations using hidden control files (e.g. `.model.safetensors.rget` next to the target).

When you interrupt a download (Ctrl+C, kill, crash, etc.), `rget` periodically saves progress to the control file. On the next run with the same command (and same output filename), it will automatically detect the partial file and resume from where it left off — even if you change the number of connections (`-n`).

### Controlling Resume Behavior

| Flag | Behavior |
|------|----------|
| (default) | Automatically resume if a valid control file exists |
| `--no-continue` | Disable resume completely for this run. No control file will be read or written |
| `--overwrite` | Force a fresh download (deletes any existing control file and truncates the target) |

Example:

```bash
# First run (gets interrupted)
rget -n 8 https://example.com/huge-model.safetensors

# Later — automatically resumes
rget -n 8 https://example.com/huge-model.safetensors

# Start over completely
rget --overwrite -n 8 https://example.com/huge-model.safetensors
```

## URL Range Expansion (Sharded Models)

When downloading many similarly named files (very common with sharded AI models), you can use **Bash-style brace expansion** directly in the URL:

```bash
rget 'https://example.com/model-{001..040}-of-00040.safetensors'
```

This will automatically expand to 40 URLs:
- `model-001-of-00040.safetensors`
- `model-002-of-00040.safetensors`
- ...
- `model-040-of-00040.safetensors`

### Zero-padding

The number of digits on the **left side** of the range determines the output width:

- `model-{001..040}-of-00040.safetensors` → `001`, `002`, ..., `040`
- `model-{1..40}-of-00040.safetensors`   → `1`, `2`, ..., `40`

This is extremely useful for model sharding where filenames use zero-padded indices.

### Multiple Ranges

You can use multiple independent ranges in one URL:

```bash
rget 'https://example.com/part-{1..8}-chunk-{01..16}.bin'
```

This expands to `8 × 16 = 128` URLs.

### Usage with Batch Mode

Range expansion works seamlessly with both positional arguments and `-i` files:

```bash
# From command line
rget 'model-{001..040}-of-00040.safetensors'

# From a file
rget -i models.txt
# models.txt can contain:
# model-{001..040}-of-00040.safetensors
# https://example.com/other-{01..05}.bin
```

## HTTP/3 (QUIC) Support

`rget` can use HTTP/3 (QUIC) when the server supports it. This can provide significantly better performance on high-latency or lossy networks.

### Enabling HTTP/3

HTTP/3 support is **optional** and must be enabled at compile time:

```bash
RUSTFLAGS="--cfg reqwest_unstable" cargo install --features http3 rget
```

Or when building from source:

```bash
RUSTFLAGS="--cfg reqwest_unstable" cargo build --release --features http3
```

### Usage

```bash
rget --http3 https://example.com/large-model.safetensors
```

When `--http3` is passed, `rget` will attempt to use HTTP/3 with prior knowledge (it will not fall back to HTTP/1.1 or HTTP/2).

### Limitations

- Requires building with `reqwest_unstable` cfg flag (HTTP/3 support in reqwest is still considered unstable).
- Only works with `rustls` (native TLS is not supported for HTTP/3).
- Binary size increases when the `http3` feature is enabled.
- QUIC support can be finicky on some networks/firewalls.

## License

MIT
