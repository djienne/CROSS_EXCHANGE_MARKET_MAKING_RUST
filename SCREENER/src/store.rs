//! The data files: `<dir>/<YYYY-MM-DD>T<HHMMSS>Z.screen.zst`, one per collection run (a run ends at
//! UTC midnight or when a missing venue rejoins), with a header then lines (`collect.rs`).
//! Concatenated zstd frames flush every 30 s; unclean stops can lose queued/unflushed data.
//! A truncated frame ends the file; complete frames remain readable. The writer thread drops
//! and counts lines when its queue is full, so feed handling does not wait on disk writes.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

const QUEUE: usize = 100_000;
const FLUSH: Duration = Duration::from_secs(30);
/// A flush also happens once this much is buffered.
const MAX_BUFFER: usize = 8 << 20;
/// zstd level: 19 costs a few ms per 30 s flush and writes ~8% less than 15.
const LEVEL: i32 = 19;

enum Msg {
    Line(String),
    Stop,
}

pub struct Store {
    tx: SyncSender<Msg>,
    dropped: Arc<AtomicU64>,
}

impl Store {
    /// Starts a new file in `dir`, named for now, beginning with `header`.
    pub fn open(dir: &Path, header: Vec<String>) -> Result<(Store, JoinHandle<Result<()>>)> {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let path = dir.join(format!("{}.screen.zst", chrono::Utc::now().format("%Y-%m-%dT%H%M%SZ")));
        let (tx, rx) = sync_channel(QUEUE);
        let writer = std::thread::Builder::new().name("store".into()).spawn(move || write(&path, header, rx))?;
        Ok((Store { tx, dropped: Arc::default() }, writer))
    }

    /// Queues one line (no newline inside).
    pub fn line(&self, line: String) {
        if let Err(TrySendError::Full(_)) = self.tx.try_send(Msg::Line(line)) {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped.is_power_of_two() {
                tracing::warn!("store: the writer is behind; {dropped} lines dropped so far");
            }
        }
    }

    /// Writes what is queued, then stops the writer.
    pub fn stop(&self) {
        let _ = self.tx.send(Msg::Stop);
    }
}

fn write(path: &Path, header: Vec<String>, rx: Receiver<Msg>) -> Result<()> {
    let mut buffer = Vec::new();
    for line in header {
        push(&mut buffer, &line);
    }
    let mut due = Instant::now() + FLUSH;
    loop {
        match rx.recv_timeout(due.saturating_duration_since(Instant::now())) {
            Ok(Msg::Line(line)) => {
                push(&mut buffer, &line);
                if buffer.len() >= MAX_BUFFER {
                    flush(path, &mut buffer)?;
                }
            }
            Ok(Msg::Stop) | Err(RecvTimeoutError::Disconnected) => return flush(path, &mut buffer),
            Err(RecvTimeoutError::Timeout) => {}
        }
        if Instant::now() >= due {
            flush(path, &mut buffer)?;
            due = Instant::now() + FLUSH;
        }
    }
}

fn push(buffer: &mut Vec<u8>, line: &str) {
    buffer.extend_from_slice(line.as_bytes());
    buffer.push(b'\n');
}

/// Appends `buffer` to `path` as one zstd frame.
fn flush(path: &Path, buffer: &mut Vec<u8>) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }
    let frame = zstd::bulk::compress(buffer, LEVEL)?;
    let mut file = OpenOptions::new().create(true).append(true).open(path).with_context(|| format!("opening {}", path.display()))?;
    file.write_all(&frame)?;
    file.sync_data()?;
    buffer.clear();
    Ok(())
}

/// The data files in `dir` whose UTC day is within `since..=until` (YYYY-MM-DD), oldest first.
pub fn files(dir: &Path, since: Option<&str>, until: Option<&str>) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            let name = p.file_name().unwrap_or_default().to_string_lossy();
            let day = &name[..10.min(name.len())];
            name.ends_with(".screen.zst") && since.is_none_or(|s| day >= s) && until.is_none_or(|u| day <= u)
        })
        .collect();
    files.sort();
    Ok(files)
}

/// A data file's lines, streamed. A frame cut short ends it.
pub fn read(path: &Path) -> Result<impl Iterator<Item = String>> {
    let decoder = zstd::stream::read::Decoder::new(File::open(path).with_context(|| format!("opening {}", path.display()))?)?;
    Ok(BufReader::new(decoder).lines().map_while(|line| line.ok()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_is_its_header_then_its_lines_and_a_cut_frame_ends_it() {
        let dir = std::env::temp_dir().join(format!("screener-store-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (store, writer) = Store::open(&dir, vec!["P\t{}".into()]).unwrap();
        store.line("B\t1\tHYPE".into());
        store.stop();
        writer.join().unwrap().unwrap();
        let [path] = files(&dir, None, None).unwrap().try_into().unwrap();
        let lines = |path: &Path| read(path).unwrap().collect::<Vec<_>>();
        assert_eq!(lines(&path), ["P\t{}", "B\t1\tHYPE"]);

        // Each flush appends a frame; a power loss mid-flush leaves half a frame at the end: the
        // lines before it still read.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&zstd::bulk::compress(b"T\t2\tHYPE\n", LEVEL).unwrap()).unwrap();
        let frame = zstd::bulk::compress(b"T\t3\tHYPE\n", LEVEL).unwrap();
        file.write_all(&frame[..frame.len() / 2]).unwrap();
        assert_eq!(lines(&path), ["P\t{}", "B\t1\tHYPE", "T\t2\tHYPE"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
