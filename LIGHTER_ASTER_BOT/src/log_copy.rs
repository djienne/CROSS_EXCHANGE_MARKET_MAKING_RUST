//! A replay copies its log into its own directory (`replay.log`), so its container can run with
//! `--rm` and leave nothing behind but its results: kept containers piled up in Docker, and two
//! cleanups saving their logs raced and lost 12 of them (2026-10-16). Lines logged before the
//! directory exists (opening the tape, ~2 s, which logs nothing unless it fails) are on stdout only.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Mutex;

static FILE: Mutex<Option<File>> = Mutex::new(None);

/// Copies every later log line to `path`, appended.
pub fn to(path: &Path) -> io::Result<()> {
    let file = File::options().create(true).append(true).open(path)?;
    *FILE.lock().unwrap_or_else(|e| e.into_inner()) = Some(file);
    Ok(())
}

/// The log's writer: stdout, plus the file once one is set. A failing copy never stops the log
/// on stdout.
pub struct Tee;

impl Write for Tee {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(file) = FILE.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            let _ = file.write_all(buf);
        }
        io::stdout().write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()
    }
}
