//! Hash verification logic (SHA-256, SHA-512) and sidecar detection.

use anyhow::{bail, Result};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::process::Command;

/// Supported hash algorithms for verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HashAlgorithm {
    Sha256,
    Sha512,
}

impl HashAlgorithm {
    pub fn name(&self) -> &'static str {
        match self {
            HashAlgorithm::Sha256 => "SHA-256",
            HashAlgorithm::Sha512 => "SHA-512",
        }
    }

    pub fn hex_len(&self) -> usize {
        match self {
            HashAlgorithm::Sha256 => 64,
            HashAlgorithm::Sha512 => 128,
        }
    }

    /// Return the list of external commands (and their args) to try, in order.
    pub fn system_tool_candidates(&self) -> &'static [(&'static str, &'static [&'static str])] {
        match self {
            HashAlgorithm::Sha256 => &[
                ("sha256sum", &[]),
                ("shasum", &["-a", "256"]),
                ("sha256", &[]),
                ("openssl", &["dgst", "-sha256", "-r"]),
            ],
            HashAlgorithm::Sha512 => &[
                ("sha512sum", &[]),
                ("shasum", &["-a", "512"]),
                ("sha512", &[]),
                ("openssl", &["dgst", "-sha512", "-r"]),
            ],
        }
    }
}

/// Try to find a sidecar hash file next to `filename` (e.g. `model.safetensors.sha256`).
pub fn find_sidecar_hash(filename: &Path) -> Option<(HashAlgorithm, String)> {
    let parent = filename.parent().unwrap_or_else(|| Path::new("."));
    let stem = filename.file_name()?.to_string_lossy();

    for (ext, algo) in [(".sha256", HashAlgorithm::Sha256), (".sha512", HashAlgorithm::Sha512)] {
        let candidate = parent.join(format!("{}{}", stem, ext));
        if let Ok(content) = std::fs::read_to_string(&candidate) {
            if let Some(hex) = parse_hash_from_sidecar(&content, algo.hex_len()) {
                return Some((algo, hex));
            }
        }
    }
    None
}

pub fn parse_hash_from_sidecar(content: &str, expected_len: usize) -> Option<String> {
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(token) = line.split_whitespace().next() {
            let token = token.trim_matches('*');
            if token.len() == expected_len && token.chars().all(|c| c.is_ascii_hexdigit()) {
                return Some(token.to_ascii_lowercase());
            }
        }
    }
    None
}

pub fn normalize_hash_hex(input: &str, algo: HashAlgorithm) -> Result<String> {
    let cleaned: String = input.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if cleaned.len() != algo.hex_len() {
        bail!(
            "Invalid {} hash length: expected {} hex characters, got {}",
            algo.name(),
            algo.hex_len(),
            cleaned.len()
        );
    }
    Ok(cleaned.to_ascii_lowercase())
}

/// Compute the hash for the given algorithm, using fast system tools when available,
/// falling back to pure Rust.
pub async fn compute_hash(
    algo: HashAlgorithm,
    path: &Path,
    spinner: &indicatif::ProgressBar,
) -> String {
    let candidates = algo.system_tool_candidates();

    for (cmd, args) in candidates {
        spinner.set_message(format!("Trying {}...", cmd));
        let mut full_args: Vec<&str> = args.to_vec();
        full_args.push(path.to_str().unwrap_or(""));
        if let Ok(output) = Command::new(cmd).args(&full_args).output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                if let Some(hex) = stdout.split_whitespace().next() {
                    let cleaned: String = hex.chars().filter(|c| c.is_ascii_hexdigit()).collect();
                    if cleaned.len() == algo.hex_len() {
                        return cleaned.to_ascii_lowercase();
                    }
                }
            }
        }
    }

    // Pure Rust fallback
    spinner.set_message(format!("Using pure-Rust {} (slower on very large files)...", algo.name()));

    let path = path.to_owned();
    let result = tokio::task::spawn_blocking(move || -> std::io::Result<String> {
        use std::fs::File;
        use std::io::{BufReader, Read};

        let file = File::open(&path)?;
        let mut reader = BufReader::with_capacity(1024 * 1024, file);
        let mut buf = [0u8; 64 * 1024];

        match algo {
            HashAlgorithm::Sha256 => {
                let mut hasher = Sha256::new();
                loop {
                    let n = reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                Ok(hex::encode(hasher.finalize()))
            }
            HashAlgorithm::Sha512 => {
                let mut hasher = sha2::Sha512::new();
                loop {
                    let n = reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                Ok(hex::encode(hasher.finalize()))
            }
        }
    })
    .await;

    match result {
        Ok(Ok(hex)) => hex,
        Ok(Err(e)) => {
            eprintln!("Rust {} failed: {}", algo.name(), e);
            "error".to_string()
        }
        Err(e) => {
            eprintln!("{} task panicked: {}", algo.name(), e);
            "error".to_string()
        }
    }
}

/// Collect all hashes we are expected to verify (from CLI flags and sidecars).
/// CLI flags take precedence over sidecars for the same algorithm.
pub fn collect_expected_hashes(
    sha256: Option<&str>,
    sha512: Option<&str>,
    filename: &Path,
) -> Result<Vec<(HashAlgorithm, String)>> {
    let mut expected: Vec<(HashAlgorithm, String)> = Vec::new();

    if let Some(h) = sha256 {
        let hex = normalize_hash_hex(h, HashAlgorithm::Sha256)?;
        expected.push((HashAlgorithm::Sha256, hex));
    }
    if let Some(h) = sha512 {
        let hex = normalize_hash_hex(h, HashAlgorithm::Sha512)?;
        expected.push((HashAlgorithm::Sha512, hex));
    }

    if let Some((algo, hex)) = find_sidecar_hash(filename) {
        let already_have = expected.iter().any(|(a, _)| *a == algo);
        if !already_have {
            expected.push((algo, hex));
        }
    }

    Ok(expected)
}
