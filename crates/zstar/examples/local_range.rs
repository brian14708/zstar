//! Print a byte range from a local archive without loading the complete file.
use std::io::{self, Write};
use std::sync::Arc;
use zstar::Archive;
use zstar::fs::AsyncLocalFileSource;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [archive_path, member, offset, length] = args.as_slice() else {
        return Err("usage: local_range ARCHIVE MEMBER OFFSET LENGTH".into());
    };
    let offset: u64 = offset.parse()?;
    let length: u64 = length.parse()?;
    let end = offset.checked_add(length).ok_or("range overflow")?;
    let source = Arc::new(AsyncLocalFileSource::open(archive_path).await?);
    let archive = Archive::open(source).await?;
    let bytes = archive.read_range(member, offset..end).await?;
    io::stdout().lock().write_all(&bytes)?;
    Ok(())
}
