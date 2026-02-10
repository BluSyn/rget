use anyhow::{bail, Context, Result};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::{header, Client};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::process::Command;
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
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .build()?;

    // HEAD to get metadata
    let head = client.head(url.as_str()).send().await?;
    if !head.status().is_success() {
        bail!(
            "HEAD failed: {} {}",
            head.status(),
            head.text().await.unwrap_or_default()
        );
    }

    let content_length = head
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .context("No Content-Length → cannot use multi-connection")?;

    let accept_ranges = head
        .headers()
        .get(header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map_or(false, |v| v.contains("bytes"));

    if !accept_ranges && args.connections > 1 {
        eprintln!("Warning: Server does not support ranges → using single connection");
    }

    let filename = args.output.unwrap_or_else(|| {
        head.headers()
            .get(header::CONTENT_DISPOSITION)
            .and_then(|v| v.to_str().ok())
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

    // Pre-allocate the file
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
    let sty = ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .progress_chars("#>-");

    let main_pb = mp.add(ProgressBar::new(content_length));
    main_pb.set_style(sty.clone());

    // Chunking
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

    // Spawn tasks — each opens its own File handle
    let mut tasks = Vec::new();
    let path_clone = filename.clone();

    for (i, (range_start, range_end)) in chunks.into_iter().enumerate() {
        let pb = mp.insert(i + 1, ProgressBar::new(range_end - range_start + 1));
        pb.set_style(sty.clone());
        pb.set_prefix(format!("Chunk {} ", i + 1));

        let main_pb_clone = main_pb.clone();
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

            while let Ok(Some(chunk)) = resp.chunk().await {
                file.write_all(&chunk).await?;
                let len = chunk.len() as u64;
                pb.inc(len);
                main_pb_clone.inc(len);
            }

            pb.finish_with_message("✓");
            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for completion
    for task in tasks {
        task.await??;
    }

    let total_duration = start_time.elapsed();
    let total_seconds = total_duration.as_secs_f64().max(0.001);
    let bytes_f64 = content_length as f64;
    let avg_speed_mib_s = bytes_f64 / total_seconds / 1_048_576.0;
    let avg_speed_mb_s = bytes_f64 / total_seconds / 1_000_000.0;

    // Compute SHA-256 using external sha256sum command
    let hash_hex = match Command::new("sha256sum").arg(&filename).output().await {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            stdout
                .split_whitespace()
                .next()
                .unwrap_or("failed-to-parse")
                .to_string()
        }
        Ok(output) => {
            eprintln!(
                "sha256sum failed with exit code: {:?}",
                output.status.code()
            );
            "error".to_string()
        }
        Err(e) => {
            eprintln!("Failed to run sha256sum: {}", e);
            "not-available".to_string()
        }
    };

    main_pb.finish_with_message("Download complete");
    mp.clear()?;

    println!("Saved to:          {}", filename.display());
    println!("Total time:        {:.2?}", total_duration);
    println!(
        "Average speed:     {:.2} MiB/s  ({:.2} MB/s)",
        avg_speed_mib_s, avg_speed_mb_s
    );
    println!("SHA-256:           {}", hash_hex);

    Ok(())
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
