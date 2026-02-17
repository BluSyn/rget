# rget

A fast, multi-connection HTTP file downloader written in Rust. Designed to fully saturate high-speed internet connections, making it ideal for downloading large files like AI models.

## Features

- Multi-connection downloads to maximize bandwidth utilization
- Progress bars for individual chunks and overall download
- Automatic SHA-256 verification after download
- Support for range requests (resumes where possible)
- Simple CLI interface

## Installation

### From Source

Ensure you have Rust installed. Then:

```bash
git clone <repository-url>
cd rget
cargo build --release
```

The binary will be in `target/release/rget`.

### From Cargo

```bash
cargo install rget
```

## Usage

```bash
rget [OPTIONS] <URL>
```

### Options

- `-o, --output <FILE>`: Output file path (default: inferred from URL or Content-Disposition header)
- `-n, --connections <NUM>`: Number of parallel connections (default: 8)
- `--min-chunk <BYTES>`: Minimum chunk size per connection in bytes (default: 1048576, 1 MiB)

### Example

```bash
rget -n 16 https://example.com/large-file.zip
```

This downloads the file using 16 parallel connections, showing progress for each chunk and overall speed.

## License

MIT
