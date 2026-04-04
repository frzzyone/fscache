use anyhow::Context;
use std::path::PathBuf;

/// Find a named file by checking next to the running binary first, then the current directory.
pub fn find_file_near_binary(filename: &str) -> anyhow::Result<PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let candidate = dir.join(filename);
            if candidate.exists() {
                return Ok(candidate);
            }
        }
    }
    let candidate = std::env::current_dir()
        .context("failed to get current directory")?
        .join(filename);
    if candidate.exists() {
        return Ok(candidate);
    }
    anyhow::bail!("{} not found next to binary or in current directory", filename)
}
