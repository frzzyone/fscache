use std::os::unix::fs::FileExt;
use std::path::PathBuf;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use tempfile::TempDir;

    fn make_tee(dir: &std::path::Path, file_size: u64) -> TeeWriter {
        let partial = dir.join("test.mkv.partial");
        let final_path = dir.join("test.mkv");
        let file = std::fs::OpenOptions::new()
            .write(true).create(true).truncate(true)
            .open(&partial).unwrap();
        TeeWriter::new(
            PathBuf::from("test.mkv"),
            partial,
            final_path,
            file,
            file_size,
        )
    }

    /// Sequential writes from offset 0 succeed and is_complete triggers at file_size.
    #[test]
    fn sequential_write_completes() {
        let dir = TempDir::new().unwrap();
        let data = b"hello world";
        let mut tee = make_tee(dir.path(), data.len() as u64);

        assert!(tee.write_sequential(data, 0));
        assert!(tee.is_complete());
    }

    /// A forward seek (gap) is rejected and returns false.
    #[test]
    fn forward_seek_aborts() {
        let dir = TempDir::new().unwrap();
        let mut tee = make_tee(dir.path(), 100);

        assert!(tee.write_sequential(b"aaaa", 0));
        // Jump forward past what we've written — this is a gap.
        assert!(!tee.write_sequential(b"bbbb", 20));
    }

    /// A backward seek within already-written data is accepted and re-writes the
    /// overlap, then resumes correctly from the new position.
    #[test]
    fn backward_seek_repositions_and_continues() {
        let dir = TempDir::new().unwrap();
        let mut tee = make_tee(dir.path(), 12);

        // Write bytes 0-7
        assert!(tee.write_sequential(b"AAAABBBB", 0));
        assert_eq!(tee.next_offset, 8);

        // Backward seek to 4 (within already-written range)
        assert!(tee.write_sequential(b"CCCC", 4));
        assert_eq!(tee.next_offset, 8);

        // Continue forward to complete the file
        assert!(tee.write_sequential(b"DDDD", 8));
        assert!(tee.is_complete());

        // Verify the partial file contains the expected bytes
        let f = tee.sync().unwrap();
        drop(f);
        let mut contents = Vec::new();
        std::fs::File::open(dir.path().join("test.mkv.partial"))
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();
        assert_eq!(&contents, b"AAAACCCCDDDD");
    }

    /// cleanup() removes the partial file.
    #[test]
    fn cleanup_removes_partial() {
        let dir = TempDir::new().unwrap();
        let tee = make_tee(dir.path(), 100);
        let partial = tee.partial_path.clone();
        assert!(partial.exists());
        tee.cleanup();
        assert!(!partial.exists());
    }

    /// sync() + rename produces the final file with the correct contents.
    #[test]
    fn sync_and_rename_produces_final_file() {
        let dir = TempDir::new().unwrap();
        let data = b"final content";
        let mut tee = make_tee(dir.path(), data.len() as u64);
        assert!(tee.write_sequential(data, 0));
        assert!(tee.is_complete());

        let partial_path = tee.partial_path.clone();
        let final_path = tee.final_path.clone();
        let _f = tee.sync().unwrap();
        drop(_f);
        std::fs::rename(&partial_path, &final_path).unwrap();

        let mut out = Vec::new();
        std::fs::File::open(&final_path).unwrap().read_to_end(&mut out).unwrap();
        assert_eq!(out, data);
    }
}

/// Writes incoming FUSE read data to the SSD cache as a side-effect, eliminating
/// the redundant NFS read that the background copy worker would otherwise perform.
///
/// Only sequential reads are tee'd. If the client seeks (offset != next_offset),
/// the tee is aborted and the file is handed off to `CacheIO` for a full copy.
pub struct TeeWriter {
    pub rel_path: PathBuf,
    pub partial_path: PathBuf,
    pub final_path: PathBuf,
    file: std::fs::File,
    pub file_size: u64,
    /// The next byte offset we expect from the client. Anything else is a seek.
    pub next_offset: u64,
}

impl TeeWriter {
    pub fn new(
        rel_path: PathBuf,
        partial_path: PathBuf,
        final_path: PathBuf,
        file: std::fs::File,
        file_size: u64,
    ) -> Self {
        Self { rel_path, partial_path, final_path, file, file_size, next_offset: 0 }
    }

    /// Attempt a sequential write. Returns `false` if a seek was detected or
    /// the write failed — caller should abort the tee in either case.
    pub fn write_sequential(&mut self, data: &[u8], offset: u64) -> bool {
        if offset != self.next_offset {
            if offset < self.next_offset {
                // Backward seek within already-written range (e.g. MKV cluster boundary
                // alignment or HTTP range re-request). Reposition and re-write — the data
                // at this range is already in the partial file, so write_at is idempotent.
                tracing::debug!(
                    "tee: backward seek for {} — repositioning from {} to {}",
                    self.rel_path.display(), self.next_offset, offset,
                );
                self.next_offset = offset;
                // fall through to write
            } else {
                // Forward seek — gap we cannot fill. Abort.
                tracing::debug!(
                    "tee: forward seek (gap) for {} — expected {}, got {}",
                    self.rel_path.display(), self.next_offset, offset,
                );
                return false;
            }
        }
        match self.file.write_at(data, offset) {
            Ok(_) => {
                self.next_offset = offset + data.len() as u64;
                true
            }
            Err(e) => {
                tracing::warn!("tee: write failed at offset {} for {}: {}", offset, self.rel_path.display(), e);
                false
            }
        }
    }

    /// Returns true once we have written the whole file.
    pub fn is_complete(&self) -> bool {
        self.next_offset >= self.file_size
    }

    /// Flush and close. Call before rename on the success path.
    pub fn sync(self) -> std::io::Result<std::fs::File> {
        self.file.sync_all()?;
        Ok(self.file)
    }

    /// Remove the partial file. Call on the abort path.
    pub fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.partial_path);
    }
}
