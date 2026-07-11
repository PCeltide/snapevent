//! Free-disk-space guard for the capture hot path.
//!
//! Capture writes plaintext JSONL continuously and must never silently fill the
//! disk (a silent failure, and worse, lost data). A [`DiskGuard`] is checked
//! before writes: it queries the filesystem's available space (via `fs4`,
//! cross-platform, no `unsafe`) and returns [`StoreError::DiskFull`] the moment
//! free space drops below a configured floor (~3 GiB by default), so the caller
//! can halt loudly rather than crash mid-write. Checking is cheap (one `statvfs`
//! / `GetDiskFreeSpaceEx`), so the capture loop can call it periodically.

use std::path::{Path, PathBuf};

use tracing::{instrument, warn};

use crate::StoreError;

/// Default minimum free space before capture halts: ~3 GiB.
pub const DEFAULT_MIN_FREE_BYTES: u64 = 3 * 1024 * 1024 * 1024;

/// Guards a capture directory against running out of disk space.
///
/// Holds the path to watch and a minimum-free-bytes floor; [`check`](Self::check)
/// queries the live free space and fails with [`StoreError::DiskFull`] when it
/// drops below the floor.
#[derive(Debug, Clone)]
pub struct DiskGuard {
    path: PathBuf,
    min_free_bytes: u64,
}

impl DiskGuard {
    /// Create a guard for `path` that fails once free space drops below
    /// `min_free_bytes`.
    pub fn new(path: impl AsRef<Path>, min_free_bytes: u64) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            min_free_bytes,
        }
    }

    /// Create a guard with the [`DEFAULT_MIN_FREE_BYTES`] floor.
    pub fn with_default_floor(path: impl AsRef<Path>) -> Self {
        Self::new(path, DEFAULT_MIN_FREE_BYTES)
    }

    /// The configured minimum free space, in bytes.
    pub fn min_free_bytes(&self) -> u64 {
        self.min_free_bytes
    }

    /// Check free space now.
    ///
    /// Returns the observed available bytes when at or above the floor, or
    /// [`StoreError::DiskFull`] (and a `warn!`) otherwise. Cheap enough to call
    /// periodically from the capture loop.
    #[instrument(skip(self), fields(path = %self.path.display(), floor = self.min_free_bytes))]
    pub fn check(&self) -> Result<u64, StoreError> {
        let available = fs4::available_space(&self.path)?;
        if available < self.min_free_bytes {
            warn!(
                available,
                floor = self.min_free_bytes,
                "free disk space below floor; capture should halt"
            );
            return Err(StoreError::DiskFull {
                path: self.path.clone(),
                available,
                floor: self.min_free_bytes,
            });
        }
        Ok(available)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_floor_is_three_gib() {
        assert_eq!(DEFAULT_MIN_FREE_BYTES, 3 * 1024 * 1024 * 1024);
    }

    #[test]
    fn passes_when_free_space_is_above_floor() {
        // Floor of 0 can never be violated, so check() must succeed and report
        // some real free space for the temp dir.
        let guard = DiskGuard::new(std::env::temp_dir(), 0);
        let available = guard.check().expect("floor 0 always passes");
        assert!(available > 0, "temp dir should report free space");
    }

    #[test]
    fn fails_with_disk_full_below_floor() {
        // A floor of u64::MAX can never be satisfied, so check() must report
        // DiskFull with the observed available space and the floor.
        let guard = DiskGuard::new(std::env::temp_dir(), u64::MAX);
        match guard.check() {
            Err(StoreError::DiskFull {
                available, floor, ..
            }) => {
                assert_eq!(floor, u64::MAX);
                assert!(available < u64::MAX, "available below the impossible floor");
            }
            other => panic!("expected DiskFull, got {other:?}"),
        }
    }
}
