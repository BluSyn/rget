// ======================================================================
// Module declarations (must be at the very top, before any `use`)
// ======================================================================
mod hash;
mod ranges;
mod resume;
mod speed;

// === Standard library ===
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// === External crates ===
use anyhow::{bail, Context, Result};
use clap::Parser;
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use parking_lot::Mutex;
use reqwest::{header, Client};

use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::net::lookup_host;
use tokio::sync::Notify;
use url::Url;

// === Internal modules (selective imports) ===
use crate::hash::HashAlgorithm;

#[derive(Parser)]
#[command(name = "rget", about = "Simple fast multi-connection HTTP downloader")]
struct Args {
    /// One or more URLs to download. Can be combined with -i/--input-file.
    #[arg(num_args = 1..)]
    urls: Vec<String>,

    /// Read URLs from a file (one URL per line). Lines starting with # are ignored.
    /// Can be combined with positional URLs.
    #[arg(short = 'i', long, value_name = "FILE")]
    input_file: Option<PathBuf>,

    /// Output file path (default: filename from URL or Content-Disposition)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of parallel connections
    #[arg(short = 'n', long, default_value_t = 8)]
    connections: usize,

    /// Minimum chunk size per connection (bytes)
    #[arg(long, default_value_t = 1_048_576)] // 1 MiB
    min_chunk: u64,

    /// Force IPv4 (like ping -4)
    #[arg(short = '4', long = "ipv4", conflicts_with = "ipv6")]
    ipv4: bool,

    /// Force IPv6 (like ping -6)
    #[arg(short = '6', long = "ipv6")]
    ipv6: bool,

    /// Skip all checksum verification and reporting after download.
    /// Mutually exclusive with --sha256 and --sha512.
    #[arg(long = "no-sha", conflicts_with_all = ["sha256", "sha512"])]
    no_sha: bool,

    /// Aggressive supervisor: once more than half of connections have
    /// finished, restart any active connection still below 50% completion.
    /// (Default supervisor only restarts when ≤2 connections remain active.)
    #[arg(long)]
    aggressive: bool,

    /// Overwrite existing output file without prompting
    #[arg(long, conflicts_with = "no_overwrite")]
    overwrite: bool,

    /// Refuse to overwrite an existing output file (exit instead of prompting)
    #[arg(long)]
    no_overwrite: bool,

    /// Disable resume support entirely for this run. No resume control file
    /// will be read from or written to, regardless of whether one exists.
    #[arg(long = "no-continue")]
    no_continue: bool,

    /// Expected SHA-256 checksum (hex). If provided, the download is verified
    /// and the process exits with an error on mismatch.
    /// Mutually exclusive with --no-sha.
    #[arg(long, value_name = "HEX", conflicts_with = "no_sha")]
    sha256: Option<String>,

    /// Expected SHA-512 checksum (hex). Same semantics as --sha256.
    /// Mutually exclusive with --no-sha.
    #[arg(long, value_name = "HEX", conflicts_with = "no_sha")]
    sha512: Option<String>,

    /// Limit the overall download speed (e.g. 50M, 2G, 500K).
    /// The limit is applied across all connections combined.
    #[arg(long, value_name = "SPEED")]
    limit_rate: Option<String>,

    /// When downloading multiple URLs, stop immediately if any download fails.
    /// By default, rget continues with the remaining URLs and exits non-zero
    /// if any download failed.
    #[arg(long)]
    fail_fast: bool,

    /// Use HTTP/3 (QUIC) with prior knowledge.
    /// This is an opt-in experimental feature.
    /// Requires compiling with: `RUSTFLAGS="--cfg reqwest_unstable" cargo build --features http3`
    #[cfg(feature = "http3")]
    #[arg(long)]
    http3: bool,
}

#[derive(Clone, Copy, Debug)]
enum IpMode {
    Auto,
    V4,
    V6,
}

impl IpMode {
    fn from_args(ipv4: bool, ipv6: bool) -> Self {
        match (ipv4, ipv6) {
            (true, false) => IpMode::V4,
            (false, true) => IpMode::V6,
            _ => IpMode::Auto,
        }
    }

    fn label(self) -> &'static str {
        match self {
            IpMode::Auto => "auto",
            IpMode::V4 => "IPv4",
            IpMode::V6 => "IPv6",
        }
    }
}

/// Resolve `host:port` honoring the IPv4/IPv6 preference.
async fn resolve_host(host: &str, port: u16, mode: IpMode) -> Result<SocketAddr> {
    let addrs: Vec<SocketAddr> = lookup_host((host, port))
        .await
        .with_context(|| format!("Failed to resolve {}", host))?
        .collect();

    if addrs.is_empty() {
        bail!("No addresses returned for {}", host);
    }

    let picked = match mode {
        IpMode::Auto => addrs.first().copied(),
        IpMode::V4 => addrs.iter().find(|a| a.is_ipv4()).copied(),
        IpMode::V6 => addrs.iter().find(|a| a.is_ipv6()).copied(),
    };

    picked.with_context(|| format!("No {} address found for {}", mode.label(), host))
}

/// Interactive `[Y/n]` prompt asking whether to overwrite an existing file.
/// Empty input is treated as Y. If stdin isn't a TTY (e.g. piped script),
/// bail with a hint to use `--overwrite` / `--no-overwrite` explicitly,
/// rather than silently overwriting or silently aborting.
async fn confirm_overwrite(path: &Path) -> Result<bool> {
    use std::io::IsTerminal;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    if !std::io::stdin().is_terminal() {
        bail!(
            "File '{}' already exists and stdin is not a TTY; \
             pass --overwrite or --no-overwrite to make the choice explicit.",
            path.display()
        );
    }

    let mut stdout = tokio::io::stdout();
    stdout
        .write_all(
            format!(
                "File '{}' already exists. Overwrite? [Y/n] ",
                path.display()
            )
            .as_bytes(),
        )
        .await?;
    stdout.flush().await?;

    let mut reader = BufReader::new(tokio::io::stdin());
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let response = line.trim().to_ascii_lowercase();

    Ok(response.is_empty() || response == "y" || response == "yes")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Collect all URLs from positional arguments and/or input file
    let mut target_urls: Vec<String> = args.urls.clone();

    if let Some(ref path) = args.input_file {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read input file: {}", path.display()))?;

        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            target_urls.push(trimmed.to_string());
        }
    }

    if target_urls.is_empty() {
        bail!("No URLs provided. Pass URLs as arguments or use -i/--input-file.");
    }

    // Expand any number ranges (e.g. model-{001..040}-of-00040.safetensors)
    let mut expanded_urls: Vec<String> = Vec::new();
    for raw in target_urls {
        expanded_urls.extend(crate::ranges::expand_ranges(&raw));
    }
    let target_urls = expanded_urls;

    // When true, we completely bypass all resume logic (no reading or writing
    // of .rget control files).
    let disable_resume = args.no_continue;

    // Create global rate limiter if --limit-rate was provided
    let rate_limiter: Option<Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>> =
        if let Some(ref speed_str) = args.limit_rate {
            let bytes_per_sec = crate::speed::parse_speed(speed_str)?;
            let quota = Quota::per_second(
                NonZeroU32::new(bytes_per_sec.clamp(1, u32::MAX as u64) as u32)
                    .context("Invalid rate limit")?,
            );
            Some(Arc::new(RateLimiter::direct(quota)))
        } else {
            None
        };

    let mut succeeded = 0usize;
    let mut failed = 0usize;

    let total = target_urls.len();

    for (i, url_str) in target_urls.iter().enumerate() {
        if total > 1 {
            println!("\n[{} / {}] {}", i + 1, total, url_str);
        }

        match download_one(url_str, &args, disable_resume, rate_limiter.clone()).await {
            Ok(()) => {
                succeeded += 1;
            }
            Err(e) => {
                eprintln!("Error downloading {}: {}", url_str, e);
                failed += 1;
                if args.fail_fast {
                    break;
                }
            }
        }
    }

    if total > 1 {
        println!("\n=== Batch Summary ===");
        println!("Total:     {}", total);
        println!("Succeeded: {}", succeeded);
        println!("Failed:    {}", failed);
    }

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

async fn download_one(
    url_str: &str,
    args: &Args,
    disable_resume: bool,
    rate_limiter: Option<Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>>,
) -> Result<()> {
    let start_time = Instant::now();

    let url = Url::parse(url_str).context("Invalid URL")?;
    let ip_mode = IpMode::from_args(args.ipv4, args.ipv6);

    // ─── Resolve hostname honoring -4 / -6 ──────────────────────────
    let host = url
        .host_str()
        .context("URL has no host to resolve")?
        .to_string();
    let port = url
        .port_or_known_default()
        .context("URL has no port and no known default for its scheme")?;
    let resolved = resolve_host(&host, port, ip_mode).await?;

    #[allow(unused_mut)]
    let mut client_builder = Client::builder()
        .user_agent("rget/0.1 (multi-connection downloader)")
        .pool_idle_timeout(Duration::from_secs(30))
        .resolve(&host, resolved);

    #[cfg(feature = "http3")]
    if args.http3 {
        // Requires `RUSTFLAGS="--cfg reqwest_unstable"` at build time
        client_builder = client_builder.http3_prior_knowledge();
    }

    let client = client_builder.build()?;

    // ─── Metadata probe ──────────────────────────────────────────────
    // Try HEAD first; fall back to a ranged GET if HEAD fails.
    // (Signed URLs — e.g. S3 presigned GETs — are bound to a single method
    // and return 401/403 on HEAD even though GET works fine. wget never
    // uses HEAD, which is why it succeeds where a HEAD-only client doesn't.)
    let (content_length, accept_ranges, content_disposition, server_etag) =
        probe_metadata(&client, url.as_str()).await?;

    if !accept_ranges && args.connections > 1 {
        eprintln!("Warning: Server does not advertise range support → using single connection");
    }

    let filename = args.output.clone().unwrap_or_else(|| {
        content_disposition
            .as_deref()
            .and_then(parse_content_disposition_filename)
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                url.path_segments()
                    .and_then(|mut seg| seg.next_back().map(String::from))
                    .filter(|s| !s.is_empty())
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("download.bin"))
            })
    });

    // Collect any hashes we must verify (from --sha256/--sha512 or sidecar files).
    // Skip entirely if the user passed --no-sha.
    let expected_hashes = if args.no_sha {
        Vec::new()
    } else {
        crate::hash::collect_expected_hashes(
            args.sha256.as_deref(),
            args.sha512.as_deref(),
            &filename,
        )?
    };

    // ─── Resume detection ────────────────────────────────────────────
    // Try to find a previous partial download for this exact target file.
    // When --no-continue is used, we skip this entirely.
    let resume_state = if disable_resume {
        None
    } else {
        load_resume_state(&filename)
    };

    let mut resuming = false;

    if let Some(state) = &resume_state {
        if !disable_resume && validate_resume_state(state, content_length, server_etag.as_deref()) {
            if filename.exists() {
                resuming = true;
                let already = state.chunks.iter().map(|c| c.written).sum::<u64>();
                println!(
                    "Resuming partial download ({:.1}% already done)...",
                    100.0 * already as f64 / content_length as f64
                );
            } else {
                // Control file exists but the target file was deleted.
                // Treat as stale and start fresh.
                remove_resume_state(&filename);
            }
        } else if disable_resume {
            // User explicitly disabled resume — leave any existing control file alone.
        }
    }

    if resuming && filename.exists() {
        // Sanity check: the file should already be the right size from previous run.
        // We do not truncate it.
    }

    println!("Downloading {} → {}", url, filename.display());
    println!(
        "Resolved {} → {} ({})",
        host,
        resolved.ip(),
        ip_mode.label()
    );
    println!(
        "Size: {} bytes | Connections: {}",
        content_length, args.connections
    );
    if args.aggressive {
        println!("Supervisor: aggressive mode");
    }

    // ─── Existing-file handling (resume-aware) ──────────────────────
    if filename.exists() {
        if resuming {
            // We have a valid resume control file for this download.
            // The target file is *expected* to exist and be partially written.
            // By default we continue the download without prompting.

            if args.no_overwrite {
                println!("Aborted: '{}' already exists.", filename.display());
                return Ok(());
            }

            // If the user explicitly passed --overwrite while a resume state
            // exists, treat it as "start over from scratch".
            if args.overwrite {
                remove_resume_state(&filename);
                resuming = false;
                // Because we set `resuming = false`, the chunking section below
                // will compute `initial_chunk_bytes` as all zeros (fresh start).
            }
        } else {
            // Not resuming (no valid control file, or --no-continue disabled resume).
            let proceed = if args.overwrite {
                true
            } else if args.no_overwrite {
                false
            } else {
                confirm_overwrite(&filename).await?
            };
            if !proceed {
                println!("Aborted: '{}' already exists.", filename.display());
                return Ok(());
            }
        }
    }

    // Pre-allocate (or open for resume)
    fs::create_dir_all(filename.parent().unwrap_or(Path::new("."))).await?;
    {
        let mut opts = OpenOptions::new();
        opts.write(true).create(true);

        if resuming {
            // Do not truncate an existing partial file when resuming.
            opts.truncate(false);
        } else {
            opts.truncate(true);
        }

        let file = opts.open(&filename).await?;
        file.set_len(content_length).await?;
    }

    let mp = MultiProgress::new();

    // Detect available parallelism once at startup (very cheap) and choose a
    // redraw rate that preserves the snappy 10 Hz feel on normal machines
    // (≥3 logical cores) while reducing terminal/CPU load on low-core or
    // container-constrained systems (1 core → 4 Hz, 2 cores → 6 Hz). The
    // underlying progress values update at full rate; only the render is
    // throttled. This respects cgroup/Docker/K8s CPU limits automatically.
    let redraw_hz = match std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
    {
        1 => 4,
        2 => 6,
        _ => 10,
    };
    mp.set_draw_target(ProgressDrawTarget::stderr_with_hz(redraw_hz));

    // ─── Main (total) progress bar ──────────────────────────────────
    let main_style = ProgressStyle::default_bar()
        .template("{spinner:.cyan} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
        .unwrap()
        .progress_chars("#>-");

    let main_pb = mp.add(ProgressBar::new(content_length));
    main_pb.set_style(main_style);

    // ─── Per-chunk style ────────────────────────────────────────────
    let chunk_style = ProgressStyle::default_bar()
        .template("{spinner:.blue} [{elapsed_precise}] [{wide_bar:.blue}] {bytes}/{total_bytes} ({bytes_per_sec})")
        .unwrap()
        .progress_chars("=>-");

    // Style applied to whichever chunk is currently the slowest.
    let chunk_style_slow = ProgressStyle::default_bar()
        .template("{spinner:.red} [{elapsed_precise}] [{wide_bar:.red}] {bytes}/{total_bytes} ({bytes_per_sec})")
        .unwrap()
        .progress_chars("=>-");

    // Style applied to chunks once they have finished downloading.
    let chunk_style_done = ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.green}] {bytes}/{total_bytes} ({bytes_per_sec})")
        .unwrap()
        .progress_chars("=>-");

    // Shared state for total speed calculation.
    //
    // `total_bytes_downloaded` is hit on every HTTP chunk callback by every
    // task, so it's an AtomicU64 (single fetch_add) instead of a Mutex.
    // `total_speed_state` holds the (last_sample_bytes, last_sample_time)
    // pair used to compute the global MiB/s message.
    // `total_sample_deadline_ms` is an atomic fast-path: every callback
    // first compares its elapsed-ms-since-start against this value, and
    // only acquires the speed-state mutex when a new sample is plausibly
    // due (~every 400 ms), eliminating ~99% of the lock acquisitions that
    // would otherwise happen at full chunk-callback rate.
    let total_bytes_downloaded = Arc::new(AtomicU64::new(0));
    let total_speed_state = Arc::new(Mutex::new((0u64, Instant::now())));
    let total_sample_deadline_ms = Arc::new(AtomicU64::new(400));
    let download_complete = Arc::new(AtomicBool::new(false));

    // ─── Chunking ───────────────────────────────────────────────────
    let effective_n = if accept_ranges {
        args.connections.max(1)
    } else {
        1
    };
    let chunk_size = (content_length / effective_n as u64).max(args.min_chunk);

    let mut chunks = Vec::new();
    let mut start = 0u64;
    while start < content_length {
        let end = (start + chunk_size - 1).min(content_length - 1);
        chunks.push((start, end));
        start = end + 1;
    }

    // Keep the chunk ranges around for periodic resume state saving in the supervisor.
    let chunk_ranges: Arc<Vec<(u64, u64)>> = Arc::new(chunks.clone());

    // When resuming, map previous progress onto the (possibly different) chunk layout.
    let initial_chunk_bytes: Vec<u64> = if resuming {
        if let Some(state) = &resume_state {
            chunks
                .iter()
                .map(|&(range_start, range_end)| {
                    compute_already_written_for_range(state, range_start, range_end)
                })
                .collect()
        } else {
            vec![0u64; chunks.len()]
        }
    } else {
        vec![0u64; chunks.len()]
    };

    // Seed global progress counters from resume state (so main progress bar and
    // speed calculations start from the correct position).
    let initial_total: u64 = initial_chunk_bytes.iter().sum();
    total_bytes_downloaded.store(initial_total, Ordering::Relaxed);

    if initial_total > 0 {
        main_pb.set_position(initial_total);
    }

    let mut tasks = Vec::new();
    let path_clone = filename.clone();

    // Per-chunk shared state used by the supervisor.
    //
    // `restart_notify` lets the supervisor cancel an in-flight attempt; the
    // task notices it via `select!` and re-issues a ranged GET starting at
    // its current write offset. `lagging_since` records when the chunk
    // first qualified as the lone laggard (so we only fire a restart after
    // the lag has been sustained for a while). `cooldown_until` blocks
    // re-evaluation right after a restart so the new connection has time
    // to ramp up before we judge it.
    struct ChunkSpeed {
        started: AtomicBool,
        done: AtomicBool,
        restart_count: AtomicUsize,
        lagging_since: Mutex<Option<Instant>>,
        cooldown_until: Mutex<Option<Instant>>,
        restart_notify: Notify,
    }
    let chunk_count = chunks.len();
    let chunk_speeds: Vec<Arc<ChunkSpeed>> = (0..chunk_count)
        .map(|_| {
            Arc::new(ChunkSpeed {
                started: AtomicBool::new(false),
                done: AtomicBool::new(false),
                restart_count: AtomicUsize::new(0),
                lagging_since: Mutex::new(None),
                cooldown_until: Mutex::new(None),
                restart_notify: Notify::new(),
            })
        })
        .collect();

    // Shared atomic counters for how many bytes have been written per chunk.
    // Used both for progress reporting and for periodically persisting the
    // resume control file.
    let chunk_written: Vec<Arc<AtomicU64>> = (0..chunk_count)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    // Seed the written counters from previous resume state (if any).
    for (i, w) in initial_chunk_bytes.iter().enumerate() {
        if i < chunk_written.len() {
            chunk_written[i].store(*w, Ordering::Relaxed);
        }
    }
    let mut chunk_pbs: Vec<ProgressBar> = Vec::with_capacity(chunk_count);

    // Style applied briefly when a chunk is being restarted.
    let chunk_style_restart = ProgressStyle::default_bar()
        .template("{spinner:.yellow} [{elapsed_precise}] [{wide_bar:.yellow}] {bytes}/{total_bytes} ({bytes_per_sec}) {msg}")
        .unwrap()
        .progress_chars("=>-");

    // Hard cap on attempts per chunk: 1 initial + 1 restart.
    const MAX_ATTEMPTS: usize = 2;

    // Zip the chunk ranges with their (possibly non-zero) starting offset when resuming.
    let chunk_iter = chunks.into_iter().zip(initial_chunk_bytes.into_iter());

    for (i, ((range_start, range_end), initial_bytes)) in chunk_iter.enumerate() {
        let pb = mp.insert(i + 1, ProgressBar::new(range_end - range_start + 1));
        pb.set_style(chunk_style.clone());
        pb.set_prefix(format!("Chunk {:2} ", i + 1));
        chunk_pbs.push(pb.clone());

        let chunk_len = range_end - range_start + 1;

        // If we are resuming and this chunk was already fully downloaded, mark it
        // done immediately and skip spawning a worker task.
        if initial_bytes >= chunk_len {
            pb.set_style(chunk_style_done.clone());
            pb.set_position(chunk_len);
            pb.finish_with_message("✓");

            let chunk_speed = chunk_speeds[i].clone();
            chunk_speed.started.store(true, Ordering::Relaxed);
            chunk_speed.done.store(true, Ordering::Relaxed);
            if i < chunk_written.len() {
                chunk_written[i].store(chunk_len, Ordering::Relaxed);
            }
            continue;
        }

        // If we are resuming this chunk (but not complete), advance the progress bar.
        if initial_bytes > 0 {
            pb.set_position(initial_bytes);
        }

        let main_pb_clone = main_pb.clone();
        let total_bytes_arc = total_bytes_downloaded.clone();
        let total_speed_state_arc = total_speed_state.clone();
        let total_sample_deadline_ms_arc = total_sample_deadline_ms.clone();
        let chunk_speed = chunk_speeds[i].clone();

        let client = client.clone();
        let url_str = url.to_string();
        let path_for_task = path_clone.clone();
        let done_style = chunk_style_done.clone();
        let restart_style = chunk_style_restart.clone();
        let download_start = start_time;
        let rate_limiter_clone = rate_limiter.clone();

        tasks.push(tokio::spawn(async move {
            chunk_speed.started.store(true, Ordering::Relaxed);

            let mut file = OpenOptions::new()
                .write(true)
                .open(&path_for_task)
                .await
                .context("Failed to open file in chunk task")?;

            // Bytes already written for this chunk (persists across restart).
            let mut chunk_bytes: u64 = initial_bytes;

            'attempts: for attempt in 0..MAX_ATTEMPTS {
                let abs_start = range_start + chunk_bytes;
                let range_header = format!("bytes={}-{}", abs_start, range_end);

                file.seek(std::io::SeekFrom::Start(abs_start))
                    .await
                    .context("Seek failed")?;

                let mut resp = client
                    .get(&url_str)
                    .header(header::RANGE, &range_header)
                    .send()
                    .await?;

                if !resp.status().is_success() {
                    anyhow::bail!("Range request failed: {}", resp.status());
                }

                // Guard against servers that ignore the Range header and
                // send 200 OK with the entire file body. Writing that
                // stream into our chunk's slot would corrupt the output.
                // 200 is only acceptable when we're effectively asking for
                // the whole file from byte 0 (i.e., single-connection mode
                // with no resume), in which case the body matches what we
                // want anyway.
                let expecting_partial = abs_start > 0 || range_end < content_length - 1;
                if expecting_partial && resp.status() != reqwest::StatusCode::PARTIAL_CONTENT {
                    anyhow::bail!(
                        "Server ignored Range header (got {} instead of 206 \
                         Partial Content) for bytes={}-{}. Refusing to write \
                         to avoid corrupting the output file.",
                        resp.status(),
                        abs_start,
                        range_end
                    );
                }

                // Reset speed-sample baseline at the start of each attempt
                // so we don't show an artificial spike right after a restart.
                let mut last_bytes = chunk_bytes;
                let mut last_time = Instant::now();
                let mut cancelled = false;

                // Only the initial attempt is cancellable. After we've used
                // our one allowed restart, we ride out the second attempt.
                let cancel_enabled = attempt + 1 < MAX_ATTEMPTS;
                let cancel_fut = chunk_speed.restart_notify.notified();
                tokio::pin!(cancel_fut);

                loop {
                    let chunk_result = if cancel_enabled {
                        tokio::select! {
                            biased;
                            _ = &mut cancel_fut => {
                                cancelled = true;
                                break;
                            }
                            r = resp.chunk() => r,
                        }
                    } else {
                        resp.chunk().await
                    };

                    if cancelled {
                        break;
                    }
                    let chunk = match chunk_result {
                        Ok(Some(c)) => c,
                        Ok(None) => break,
                        Err(e) => return Err(e.into()),
                    };

                    // Apply global bandwidth limit if configured
                    if let Some(ref limiter) = rate_limiter_clone {
                        let len = chunk.len() as u32;
                        if let Some(nonzero_len) = NonZeroU32::new(len) {
                            let _ = limiter.until_n_ready(nonzero_len).await;
                        }
                    }

                    file.write_all(&chunk).await?;
                    let len = chunk.len() as u64;
                    chunk_bytes += len;

                    pb.inc(len);

                    let total_snapshot = total_bytes_arc.fetch_add(len, Ordering::Relaxed) + len;
                    main_pb_clone.set_position(total_snapshot);

                    let now = Instant::now();
                    if now.duration_since(last_time) >= Duration::from_millis(400) {
                        let delta_bytes = chunk_bytes.saturating_sub(last_bytes);
                        let delta_time = now.duration_since(last_time).as_secs_f64().max(0.001);
                        let speed_mib_s = (delta_bytes as f64) / delta_time / 1_048_576.0;

                        pb.set_message(format!("{:.1} MiB/s", speed_mib_s));

                        last_bytes = chunk_bytes;
                        last_time = now;
                    }

                    // Global speed sample, gated by an atomic deadline so
                    // we only take the speed-state lock once per ~400 ms
                    // across all tasks instead of on every chunk callback.
                    let elapsed_ms = now.duration_since(download_start).as_millis() as u64;
                    if elapsed_ms >= total_sample_deadline_ms_arc.load(Ordering::Relaxed) {
                        let mut state = total_speed_state_arc.lock();
                        let (last_total_bytes, last_total_time) = &mut *state;
                        let delta_t = now.duration_since(*last_total_time).as_secs_f64();
                        if delta_t >= 0.4 {
                            let delta_total = total_snapshot.saturating_sub(*last_total_bytes);
                            let total_speed =
                                (delta_total as f64) / delta_t.max(0.001) / 1_048_576.0;
                            main_pb_clone.set_message(format!("{:.1} MiB/s", total_speed));

                            *last_total_bytes = total_snapshot;
                            *last_total_time = now;
                            total_sample_deadline_ms_arc.store(elapsed_ms + 400, Ordering::Relaxed);
                        }
                    }
                }

                if cancelled {
                    // Drop the response (closes connection) and reset state
                    // for the next attempt.
                    drop(resp);
                    chunk_speed.restart_count.fetch_add(1, Ordering::Relaxed);
                    *chunk_speed.lagging_since.lock() = None;
                    *chunk_speed.cooldown_until.lock() =
                        Some(Instant::now() + Duration::from_secs(15));

                    pb.set_style(restart_style.clone());
                    pb.set_message("restarting…");
                    pb.println(format!(
                        "Chunk {:2}: restarting at {}/{} bytes ({:.1}%)",
                        i + 1,
                        chunk_bytes,
                        range_end - range_start + 1,
                        100.0 * chunk_bytes as f64 / (range_end - range_start + 1) as f64
                    ));
                    continue 'attempts;
                }

                break;
            }

            chunk_speed.done.store(true, Ordering::Relaxed);
            pb.set_style(done_style);
            pb.finish_with_message("✓");
            Ok::<_, anyhow::Error>(())
        }));
    }

    // ─── Supervisor: highlight slowest chunk + restart stuck chunks ─
    // Highlighting: once total download passes HIGHLIGHT_AFTER_FRACTION,
    // the active chunk with the lowest completion is shown red. Stable
    // and flicker-free.
    //
    // Hung-connection detection (mode-independent):
    //   - Any active chunk that has transferred fewer than
    //     HUNG_BYTES_THRESHOLD bytes in HUNG_DURATION_SECS is force-restarted,
    //     subject to the same restart-count + cooldown guards as below.
    //
    // Restart conditions (default mode): only fires under tight conditions
    // to avoid wasted work.
    //   - At most 2 chunks still active (everyone else is done) AND
    //     at least 1 chunk has finished (the link demonstrably works), AND
    //   - Laggard's completion < 50%, AND
    //   - The lag has been sustained for ≥10s (no transient stalls), AND
    //   - The chunk hasn't already been restarted, AND
    //   - We're not in the post-restart cooldown window.
    //
    // Restart conditions (--aggressive mode):
    //   - At least half of all chunks have finished, AND
    //   - Laggard's completion < 50%, AND
    //   - Lag sustained ≥5s, AND
    //   - Same restart-count + cooldown guards as default.
    const HIGHLIGHT_AFTER_FRACTION: f64 = 0.10;
    const RESTART_FRACTION_CEILING: f64 = 0.50;
    const RESTART_SUSTAINED_SECS_DEFAULT: u64 = 10;
    const RESTART_SUSTAINED_SECS_AGGRESSIVE: u64 = 5;
    const HUNG_DURATION_SECS: u64 = 15;
    const HUNG_BYTES_THRESHOLD: u64 = 64 * 1024; // 64 KiB

    /// How often (in seconds) the supervisor writes the current download
    /// progress to the resume control file. Lower values give better
    /// crash recovery at the cost of more disk I/O.
    const RESUME_SAVE_INTERVAL_SECS: u64 = 5;

    let aggressive = args.aggressive;
    let supervisor_speeds = chunk_speeds.clone();
    let supervisor_pbs = chunk_pbs.clone();
    let supervisor_total = total_bytes_downloaded.clone();
    let supervisor_chunk_ranges = chunk_ranges.clone();
    let supervisor_url = url.to_string();
    let supervisor_etag = server_etag.clone();
    let supervisor_filename = filename.clone();
    let normal_style = chunk_style.clone();
    let slow_style = chunk_style_slow.clone();
    let supervisor_download_complete = download_complete.clone();
    let supervisor_disable_resume = disable_resume;
    let supervisor_content_length = content_length;
    let supervisor = tokio::spawn(async move {
        // 800 ms tick reduces CPU wakeups vs 500 ms while still giving
        // sub-second detection latency for hung connections (the hung
        // threshold itself is 15 s, so this is plenty responsive).
        let mut tick = tokio::time::interval(Duration::from_millis(800));
        tick.tick().await; // discard the immediate first tick
        let mut current_slow: Vec<bool> = vec![false; supervisor_speeds.len()];
        let mut last_resume_save = Instant::now();
        // Per-chunk (last_observed_position, last_observed_time) used by the
        // hung-connection detector. Lazy-initialised the first time we see
        // each chunk active.
        let mut last_progress: Vec<Option<(u64, Instant)>> = vec![None; supervisor_speeds.len()];
        loop {
            tick.tick().await;

            if supervisor_download_complete.load(Ordering::Relaxed) {
                break;
            }

            let now = Instant::now();

            // ── Periodic resume state saving (throttled) ─────────────
            // We do this early in the loop (before any `continue`s) so that
            // progress is persisted even when there are few/no active chunks
            // left, or when using small RESUME_SAVE_INTERVAL_SECS values.
            if !supervisor_disable_resume
                && now.duration_since(last_resume_save)
                    >= Duration::from_secs(RESUME_SAVE_INTERVAL_SECS)
            {
                last_resume_save = now;

                let current_written: Vec<u64> =
                    supervisor_pbs.iter().map(|pb| pb.position()).collect();

                let progress: Vec<ChunkProgress> = supervisor_chunk_ranges
                    .iter()
                    .zip(current_written.iter())
                    .map(|(&(start, end), &written)| ChunkProgress {
                        start,
                        end,
                        written: written.min(end - start + 1),
                    })
                    .collect();

                let state = ResumeState {
                    version: RESUME_STATE_VERSION,
                    url: supervisor_url.clone(),
                    etag: supervisor_etag.clone(),
                    content_length: supervisor_content_length,
                    connections: supervisor_speeds.len(),
                    min_chunk: 0,
                    chunks: progress,
                };

                let _ = save_resume_state(&state, &supervisor_filename);
            }

            let total_so_far = supervisor_total.load(Ordering::Relaxed);
            let overall_fraction = total_so_far as f64 / content_length as f64;

            // Build the active list once; reuse for both highlight + restart.
            let mut active: Vec<(usize, f64)> = supervisor_speeds
                .iter()
                .enumerate()
                .filter_map(|(i, s)| {
                    if !s.started.load(Ordering::Relaxed) || s.done.load(Ordering::Relaxed) {
                        return None;
                    }
                    let pb = &supervisor_pbs[i];
                    let len = pb.length().unwrap_or(0);
                    if len == 0 {
                        return None;
                    }
                    let frac = pb.position() as f64 / len as f64;
                    Some((i, frac))
                })
                .collect();
            active.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

            let finished_count = supervisor_speeds
                .iter()
                .filter(|s| s.done.load(Ordering::Relaxed))
                .count();

            // ── Slowest-chunk highlight ─────────────────────────────
            let mut want_slow = vec![false; supervisor_speeds.len()];
            if overall_fraction >= HIGHLIGHT_AFTER_FRACTION && active.len() >= 2 {
                want_slow[active[0].0] = true;
            }
            for (i, pb) in supervisor_pbs.iter().enumerate() {
                if supervisor_speeds[i].done.load(Ordering::Relaxed) {
                    continue;
                }
                // Don't override the yellow restart style.
                if supervisor_speeds[i].restart_count.load(Ordering::Relaxed) > 0
                    && supervisor_speeds[i]
                        .cooldown_until
                        .lock()
                        .is_some_and(|t| Instant::now() < t)
                {
                    continue;
                }
                if want_slow[i] != current_slow[i] {
                    pb.set_style(if want_slow[i] {
                        slow_style.clone()
                    } else {
                        normal_style.clone()
                    });
                    current_slow[i] = want_slow[i];
                }
            }

            // ── Hung-connection detection (mode-independent) ────────
            // Force-restart any active chunk that hasn't transferred at
            // least HUNG_BYTES_THRESHOLD bytes within HUNG_DURATION_SECS.
            // Same restart-count + cooldown guards as the lag-based path,
            // so MAX_ATTEMPTS is preserved (no endless loop).
            for &(i, _) in &active {
                let state = &supervisor_speeds[i];
                if state.restart_count.load(Ordering::Relaxed) >= 1 {
                    continue;
                }
                let pb = &supervisor_pbs[i];
                let pos = pb.position();
                let entry = last_progress[i].get_or_insert((pos, now));
                let bytes_progressed = pos.saturating_sub(entry.0);
                if bytes_progressed >= HUNG_BYTES_THRESHOLD {
                    *entry = (pos, now);
                    continue;
                }
                if now.duration_since(entry.1) < Duration::from_secs(HUNG_DURATION_SECS) {
                    continue;
                }
                if state.cooldown_until.lock().is_some_and(|t| now < t) {
                    continue;
                }
                pb.println(format!(
                    "Chunk {:2}: hung (<{} bytes in {}s) — forcing restart",
                    i + 1,
                    HUNG_BYTES_THRESHOLD,
                    HUNG_DURATION_SECS
                ));
                state.restart_notify.notify_one();
                // Reset our local baseline so we don't re-fire on the next
                // tick before the new connection has had a chance to ramp
                // up. (The cooldown guard above is the authoritative gate;
                // this just keeps the local tracker tidy.)
                *entry = (pos, now);
            }

            // ── Restart trigger ─────────────────────────────────────
            // Mode-specific gating: default requires the laggard to be
            // nearly alone among active chunks; aggressive only requires
            // a finished majority.
            if active.is_empty() {
                continue;
            }
            let chunk_count = supervisor_speeds.len();
            let gate_ok = if aggressive {
                finished_count >= chunk_count / 2
            } else {
                active.len() <= 2 && finished_count >= 1
            };
            if !gate_ok {
                continue;
            }
            let (slowest_idx, slowest_frac) = active[0];
            let state = &supervisor_speeds[slowest_idx];

            if state.restart_count.load(Ordering::Relaxed) >= 1 {
                continue;
            }
            if let Some(t) = *state.cooldown_until.lock() {
                if Instant::now() < t {
                    continue;
                }
            }

            let frac_ok = slowest_frac < RESTART_FRACTION_CEILING;
            let sustained_secs = if aggressive {
                RESTART_SUSTAINED_SECS_AGGRESSIVE
            } else {
                RESTART_SUSTAINED_SECS_DEFAULT
            };

            if frac_ok {
                let mut lag = state.lagging_since.lock();
                let since = lag.get_or_insert_with(Instant::now);
                if Instant::now().duration_since(*since) >= Duration::from_secs(sustained_secs) {
                    drop(lag);
                    state.restart_notify.notify_one();
                }
            } else {
                *state.lagging_since.lock() = None;
            }
        }
    });

    // Wait for all chunks
    for task in tasks {
        task.await??;
    }

    // Download succeeded — remove the resume control file so it doesn't linger.
    remove_resume_state(&filename);

    // Signal supervisor to exit cleanly instead of aborting mid-tick.
    // This avoids leaving progress bars in an inconsistent state on slow terminals.
    download_complete.store(true, Ordering::Relaxed);
    // Give the supervisor one final tick to observe the flag and break.
    tokio::time::sleep(Duration::from_millis(50)).await;
    supervisor.abort(); // still abort in case it's blocked on a long pb.println
    let _ = supervisor.await;

    // ─── Summary ─────────────────────────────────────────────────────
    let total_duration = start_time.elapsed();
    let total_seconds = total_duration.as_secs_f64().max(0.001);
    let avg_speed_mib_s = (content_length as f64) / total_seconds / 1_048_576.0;
    let avg_speed_mb_s = (content_length as f64) / total_seconds / 1_000_000.0;

    main_pb.finish_with_message("Download complete");
    mp.clear()?;

    println!("Saved to:          {}", filename.display());
    println!("Total time:        {:.2?}", total_duration);
    println!(
        "Average speed:     {:.2} MiB/s  ({:.2} MB/s)",
        avg_speed_mib_s, avg_speed_mb_s
    );

    // ─── Hash verification / reporting ──────────────────────────────
    if expected_hashes.is_empty() {
        if args.no_sha {
            return Ok(());
        }
        // Legacy behavior: just compute and print SHA-256 for the user
        println!("Computing SHA-256...");

        let hash_spinner = ProgressBar::new_spinner();
        hash_spinner.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        hash_spinner.enable_steady_tick(Duration::from_millis(120));
        hash_spinner.set_message("Hashing file...");

        let hash_hex = crate::hash::compute_hash(HashAlgorithm::Sha256, &filename, &hash_spinner).await;

        hash_spinner.finish_and_clear();
        println!("SHA-256:           {}", hash_hex);
        return Ok(());
    }

    // We have one or more hashes to verify (from CLI or sidecars).
    println!("Verifying checksum(s)...");

    let hash_spinner = ProgressBar::new_spinner();
    hash_spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    hash_spinner.enable_steady_tick(Duration::from_millis(120));

    let mut all_ok = true;
    for (algo, expected_hex) in &expected_hashes {
        hash_spinner.set_message(format!("Computing {}...", algo.name()));
        let actual = crate::hash::compute_hash(*algo, &filename, &hash_spinner).await;

        if actual == *expected_hex {
            println!("{}: {}  ✓", algo.name(), actual);
        } else {
            eprintln!(
                "{} mismatch!\n  Expected: {}\n  Actual:   {}",
                algo.name(),
                expected_hex,
                actual
            );
            all_ok = false;
        }
    }

    hash_spinner.finish_and_clear();

    if !all_ok {
        bail!("Checksum verification failed");
    }

    Ok(())
}

/// Probe a URL for metadata needed for multi-connection download.
///
/// Returns: `(content_length, accept_ranges, content_disposition, etag)`.
///
/// Tries HEAD first, then falls back to a `Range: bytes=0-0` GET if HEAD
/// returns a non-success status. The fallback handles signed URLs that
/// are bound to GET (e.g. S3 presigned URLs) and servers that don't
/// implement HEAD at all.
async fn probe_metadata(
    client: &Client,
    url: &str,
) -> Result<(u64, bool, Option<String>, Option<String>)> {
    let head = client.head(url).send().await?;

    if head.status().is_success() {
        let cl = head
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .context("No Content-Length → cannot use multi-connection")?;
        let ar = head
            .headers()
            .get(header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.contains("bytes"));
        let cd = head
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let etag = head
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        return Ok((cl, ar, cd, etag));
    }

    eprintln!(
        "HEAD returned {} → falling back to ranged GET probe",
        head.status()
    );
    drop(head);

    let probe = client
        .get(url)
        .header(header::RANGE, "bytes=0-0")
        .send()
        .await?;

    if !probe.status().is_success() {
        bail!(
            "Probe failed: {} {}",
            probe.status(),
            probe.text().await.unwrap_or_default()
        );
    }

    let cd = probe
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let etag = probe
        .headers()
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    if probe.status() == reqwest::StatusCode::PARTIAL_CONTENT {
        // 206: ranges supported. Total size lives in `Content-Range: bytes 0-0/<total>`.
        let total = probe
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit('/').next().map(str::trim))
            .filter(|v| *v != "*")
            .and_then(|v| v.parse::<u64>().ok())
            .context("Probe returned 206 without parseable Content-Range total")?;
        Ok((total, true, cd, etag))
    } else {
        // 200: server ignored Range. Single-connection only.
        let total = probe
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .context("No Content-Length → cannot use multi-connection")?;
        Ok((total, false, cd, etag))
    }
}

fn parse_content_disposition_filename(cd: &str) -> Option<String> {
    if let Some(pos) = cd.find("filename=") {
        let mut val = cd[pos + 9..].trim_start_matches('"').to_string();
        if let Some(end_quote) = val.find('"') {
            val.truncate(end_quote);
        }
        if let Some(semi) = val.find(';') {
            val.truncate(semi);
        }
        let cleaned = val.replace("%20", " ");
        // Return only the basename component; never trust a path with separators
        // coming from an untrusted server.
        std::path::Path::new(&cleaned)
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty() && s != "." && s != "..")
    } else {
        None
    }
}

/// Compute SHA-256 of a file, preferring fast native system tools when available.
/// Falls back to a pure-Rust implementation (`sha2` crate) if no system hasher works.
/// This makes the feature portable across Linux, macOS, Windows, and minimal containers.

// Hash types and functions have been moved to src/hash.rs

// Old hash code removed — now lives in src/hash.rs


/// Collect all hashes we are expected to verify (from CLI flags and sidecars).
/// CLI flags take precedence over sidecars for the same algorithm.
// collect_expected_hashes moved to src/hash.rs


// normalize_hash_hex moved to src/hash.rs
// compute_hash and related functions have been moved to src/hash.rs


// ============================================================================
// Cross-run resume support (control file)
// Resume types are now in src/resume.rs
use crate::resume::{
    compute_already_written_for_range, load_resume_state, remove_resume_state, save_resume_state,
    validate_resume_state, ChunkProgress, ResumeState, RESUME_STATE_VERSION,
};

// Resume helper functions have been moved to src/resume.rs
// (control_path_for, load/save/remove/validate_resume_state, compute_already_written_for_range)

