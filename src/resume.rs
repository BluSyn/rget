//! Resume state and control file handling.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Version of the resume control file format.
pub const RESUME_STATE_VERSION: u32 = 1;

/// Maximum allowed content length in a resume state file.
/// This should match or be lower than the value used in main.rs.
const MAX_CONTENT_LENGTH: u64 = 2 * 1024 * 1024 * 1024 * 1024; // 2 TiB

/// Per-chunk progress stored in the control file.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkProgress {
    /// Inclusive start byte offset of this chunk.
    pub start: u64,
    /// Inclusive end byte offset of this chunk.
    pub end: u64,
    /// How many bytes have been successfully written for this chunk so far.
    pub written: u64,
}

/// Persistent state for resuming a download across `rget` invocations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResumeState {
    pub version: u32,
    /// The original URL being downloaded.
    pub url: String,
    /// ETag returned by the server (if any).
    pub etag: Option<String>,
    pub content_length: u64,
    /// Number of connections used when this state was created.
    pub connections: usize,
    pub min_chunk: u64,
    /// Progress for each chunk.
    pub chunks: Vec<ChunkProgress>,
}

/// Returns the path of the control file for a given download target.
pub fn control_path_for(target: &Path) -> PathBuf {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target.file_name().unwrap_or_default();
    parent.join(format!(".{}.rget", name.to_string_lossy()))
}

/// Attempt to load and deserialize a resume control file.
pub fn load_resume_state(target: &Path) -> Option<ResumeState> {
    let control_path = control_path_for(target);
    if !control_path.exists() {
        return None;
    }

    match std::fs::read_to_string(&control_path) {
        Ok(content) => match serde_json::from_str::<ResumeState>(&content) {
            Ok(state) => {
                if state.version == RESUME_STATE_VERSION {
                    Some(state)
                } else {
                    None
                }
            }
            Err(_) => None,
        },
        Err(_) => None,
    }
}

/// Atomically write (or update) the resume control file.
pub fn save_resume_state(state: &ResumeState, target: &Path) -> Result<()> {
    let control_path = control_path_for(target);
    let tmp_path = control_path.with_extension("rget.tmp");

    let json = serde_json::to_string_pretty(state)
        .context("Failed to serialize resume state")?;

    std::fs::write(&tmp_path, json).context("Failed to write temporary resume file")?;

    if let Ok(file) = std::fs::OpenOptions::new().write(true).open(&tmp_path) {
        let _ = file.sync_all();
    }

    std::fs::rename(&tmp_path, &control_path)
        .context("Failed to atomically replace resume control file")?;

    Ok(())
}

/// Remove the control file.
pub fn remove_resume_state(target: &Path) {
    let control_path = control_path_for(target);
    let _ = std::fs::remove_file(control_path);
}

/// Validate whether a loaded `ResumeState` is still usable.
/// This includes security checks against tampered or malicious control files.
pub fn validate_resume_state(
    state: &ResumeState,
    content_length: u64,
    etag: Option<&str>,
    max_content_length: u64,
) -> bool {
    // Basic length match
    if state.content_length != content_length {
        return false;
    }

    // Security: Reject absurd content lengths from corrupted/malicious resume files
    if state.content_length > max_content_length {
        return false;
    }

    // ETag validation (strong indicator the file changed on the server)
    if let (Some(stored_etag), Some(current_etag)) = (&state.etag, etag) {
        if stored_etag != current_etag {
            return false;
        }
    }

    // Per-chunk sanity checks (prevent malicious written values)
    for cp in &state.chunks {
        let chunk_size = cp.end.saturating_sub(cp.start) + 1;
        if cp.written > chunk_size {
            return false;
        }
    }

    true
}

/// Given an old `ResumeState`, compute how many bytes in the range have already been written.
pub fn compute_already_written_for_range(
    state: &ResumeState,
    range_start: u64,
    range_end: u64,
) -> u64 {
    let mut written = 0u64;

    for cp in &state.chunks {
        let overlap_start = range_start.max(cp.start);
        let overlap_end = range_end.min(cp.end);

        if overlap_start <= overlap_end {
            let already = cp.written.min(cp.end - cp.start + 1);
            let covered = already.saturating_sub(overlap_start.saturating_sub(cp.start));
            written += covered.min(overlap_end - overlap_start + 1);
        }
    }

    written.min(range_end - range_start + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resume_state_with_huge_content_length_is_invalid() {
        let state = ResumeState {
            version: RESUME_STATE_VERSION,
            url: "https://example.com/huge.bin".to_string(),
            etag: None,
            content_length: 10_000_000_000_000, // 10 TB
            connections: 8,
            min_chunk: 1_048_576,
            chunks: vec![],
        };

        assert!(!validate_resume_state(&state, 10_000_000_000_000, None, MAX_CONTENT_LENGTH));
    }

    #[test]
    fn test_resume_state_with_written_beyond_chunk_size_is_invalid() {
        let state = ResumeState {
            version: RESUME_STATE_VERSION,
            url: "https://example.com/test.bin".to_string(),
            etag: None,
            content_length: 1000,
            connections: 1,
            min_chunk: 1000,
            chunks: vec![ChunkProgress {
                start: 0,
                end: 999,
                written: 5000, // Clearly malicious
            }],
        };

        assert!(!validate_resume_state(&state, 1000, None, MAX_CONTENT_LENGTH));
    }

    #[test]
    fn test_normal_resume_state_is_valid() {
        let state = ResumeState {
            version: RESUME_STATE_VERSION,
            url: "https://example.com/model.bin".to_string(),
            etag: Some("\"abc123\"".to_string()),
            content_length: 10_000_000,
            connections: 8,
            min_chunk: 1_048_576,
            chunks: vec![
                ChunkProgress { start: 0, end: 5_000_000, written: 5_000_000 },
                ChunkProgress { start: 5_000_001, end: 10_000_000, written: 2_000_000 },
            ],
        };

        assert!(validate_resume_state(&state, 10_000_000, Some("\"abc123\""), MAX_CONTENT_LENGTH));
    }

    #[test]
    fn test_resume_state_respects_custom_max_size() {
        let state = ResumeState {
            version: RESUME_STATE_VERSION,
            url: "https://example.com/file.bin".to_string(),
            etag: None,
            content_length: 50_000_000_000, // 50 GB
            connections: 4,
            min_chunk: 1_048_576,
            chunks: vec![],
        };

        // Should be invalid with default 2TiB? Wait, 50GB is fine. Let's test rejection with small limit
        assert!(!validate_resume_state(&state, 50_000_000_000, None, 10_000_000_000)); // 10GB cap
        assert!(validate_resume_state(&state, 50_000_000_000, None, 100_000_000_000)); // 100GB cap
    }
}
