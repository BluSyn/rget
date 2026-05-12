use anyhow::{bail, Context, Result};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::{header, Client};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::Mutex;
use url::Url;

#[derive(Parser)]
#[command(name = "rget", about = "Simple fast multi-connection HTTP downloader")]
struct Args {
    /// The URL to download
    url: String,

    /// Output file path (default: filename from URL or Content-Disposition)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of parallel connections
    #[arg(short = 'n', long, default_value_t = 8)]
    connections: usize,

    /// Minimum chunk size per connection (bytes)
    #[arg(long, default_value_t = 1_048_576)] // 1 MiB
    min_chunk: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let start_time = Instant::now();

    let url = Url::parse(&args.url).context("Invalid URL")?;
    let client = Client::builder()
        .user_agent("rget/0.1 (multi-connection downloader)")
        .pool_idle_timeout(Duration::from_secs(30))
        .build()?;

    // ─── Metadata probe ──────────────────────────────────────────────
    // Try HEAD first; fall back to a ranged GET if HEAD fails.
    // (Signed URLs — e.g. S3 presigned GETs — are bound to a single method
    // and return 401/403 on HEAD even though GET works fine. wget never
    // uses HEAD, which is why it succeeds where a HEAD-only client doesn't.)
    let (content_length, accept_ranges, content_disposition) =
        probe_metadata(&client, url.as_str()).await?;

    if !accept_ranges && args.connections > 1 {
        eprintln!("Warning: Server does not advertise range support → using single connection");
    }

    let filename = args.output.unwrap_or_else(|| {
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

    println!("Downloading {} → {}", url, filename.display());
    println!(
        "Size: {} bytes | Connections: {}",
        content_length, args.connections
    );

    // Pre-allocate
    fs::create_dir_all(filename.parent().unwrap_or(Path::new("."))).await?;
    {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&filename)
            .await?;
        file.set_len(content_length).await?;
    }

    let mp = MultiProgress::new();

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

    // Shared state for total speed calculation
    let total_bytes_downloaded = Arc::new(Mutex::new(0u64));
    let total_last_bytes = Arc::new(Mutex::new(0u64));
    let total_last_time = Arc::new(Mutex::new(Instant::now()));

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

    let mut tasks = Vec::new();
    let path_clone = filename.clone();

    for (i, (range_start, range_end)) in chunks.into_iter().enumerate() {
        let pb = mp.insert(i + 1, ProgressBar::new(range_end - range_start + 1));
        pb.set_style(chunk_style.clone());
        pb.set_prefix(format!("Chunk {:2} ", i + 1));

        let main_pb_clone = main_pb.clone();
        let total_bytes_arc = total_bytes_downloaded.clone();
        let total_last_bytes_arc = total_last_bytes.clone();
        let total_last_time_arc = total_last_time.clone();

        let client = client.clone();
        let url_str = url.to_string();
        let range_header = format!("bytes={}-{}", range_start, range_end);
        let path_for_task = path_clone.clone();

        tasks.push(tokio::spawn(async move {
            let mut resp = client
                .get(&url_str)
                .header(header::RANGE, &range_header)
                .send()
                .await?;

            if !resp.status().is_success() {
                anyhow::bail!("Range request failed: {}", resp.status());
            }

            let mut file = OpenOptions::new()
                .write(true)
                .open(&path_for_task)
                .await
                .context("Failed to open file in chunk task")?;

            file.seek(std::io::SeekFrom::Start(range_start))
                .await
                .context("Seek failed")?;

            let mut chunk_bytes = 0u64;
            let mut last_bytes = 0u64;
            let mut last_time = Instant::now();

            while let Ok(Some(chunk)) = resp.chunk().await {
                file.write_all(&chunk).await?;

                let len = chunk.len() as u64;
                chunk_bytes += len;

                // Update chunk bar
                pb.inc(len);

                // Update total
                {
                    let mut total = total_bytes_arc.lock().await;
                    *total += len;
                    main_pb_clone.set_position(*total);
                }

                // Update total speed ~every 400ms
                let now = Instant::now();
                if now.duration_since(last_time) >= Duration::from_millis(400) {
                    let delta_bytes = chunk_bytes - last_bytes;
                    let delta_time = now.duration_since(last_time).as_secs_f64().max(0.001);
                    let speed_mib_s = (delta_bytes as f64) / delta_time / 1_048_576.0;

                    pb.set_message(format!("{:.1} MiB/s", speed_mib_s));

                    last_bytes = chunk_bytes;
                    last_time = now;
                }

                // Update global total speed
                {
                    let mut last_total_bytes = total_last_bytes_arc.lock().await;
                    let mut last_total_time = total_last_time_arc.lock().await;

                    let delta_total = chunk_bytes - *last_total_bytes;
                    let delta_t = now
                        .duration_since(*last_total_time)
                        .as_secs_f64()
                        .max(0.001);

                    if delta_t > 0.0 {
                        let total_speed = (delta_total as f64) / delta_t / 1_048_576.0;
                        main_pb_clone.set_message(format!("{:.1} MiB/s", total_speed));
                    }

                    *last_total_bytes = chunk_bytes;
                    *last_total_time = now;
                }
            }

            pb.finish_with_message("✓");
            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for all chunks
    for task in tasks {
        task.await??;
    }

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

    // ─── SHA-256 with spinner ───────────────────────────────────────
    println!("Computing SHA-256...");

    let hash_spinner = ProgressBar::new_spinner();
    hash_spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    hash_spinner.enable_steady_tick(Duration::from_millis(120));
    hash_spinner.set_message("Running sha256sum...");

    let output = Command::new("sha256sum").arg(&filename).output().await;

    hash_spinner.finish_and_clear();

    let hash_hex = match output {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .next()
            .unwrap_or("parse-error")
            .to_string(),
        Ok(out) => {
            eprintln!("sha256sum exited with code {:?}", out.status.code());
            "error".to_string()
        }
        Err(e) => {
            eprintln!("Cannot run sha256sum: {}", e);
            "not-available".to_string()
        }
    };

    println!("SHA-256:           {}", hash_hex);

    Ok(())
}

/// Probe a URL for `(content_length, accept_ranges, content_disposition)`.
///
/// Tries HEAD first, then falls back to a `Range: bytes=0-0` GET if HEAD
/// returns a non-success status. The fallback handles signed URLs that
/// are bound to GET (e.g. S3 presigned URLs) and servers that don't
/// implement HEAD at all.
async fn probe_metadata(
    client: &Client,
    url: &str,
) -> Result<(u64, bool, Option<String>)> {
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
            .map_or(false, |v| v.contains("bytes"));
        let cd = head
            .headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        return Ok((cl, ar, cd));
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
        Ok((total, true, cd))
    } else {
        // 200: server ignored Range. Single-connection only.
        let total = probe
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .context("No Content-Length → cannot use multi-connection")?;
        Ok((total, false, cd))
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
        Some(val.replace("%20", " "))
    } else {
        None
    }
}
