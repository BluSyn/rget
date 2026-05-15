//! Resume state and control file handling.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Version of the resume control file format.
pub const RESUME_STATE_VERSION: u32 = 1;

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
pub fn validate_resume_state(
    state: &ResumeState,
    content_length: u64,
    etag: Option<&str>,
) -> bool {
    if state.content_length != content_length {
        return false;
    }

    if let (Some(stored_etag), Some(current_etag)) = (&state.etag, etag) {
        if stored_etag != current_etag {
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
