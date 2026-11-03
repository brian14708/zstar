//! Write an archive to a file, read its contents, and verify it.
use std::sync::Arc;
use zstar::fs::AsyncLocalFileSource;
use zstar::{Archive, Builder, VerificationScope};

#[tokio::main]
async fn main() -> zstar::Result<()> {
    // region create
    let output = tokio::fs::File::create("example.zstar").await?;
    let mut builder = Builder::new(output);
    builder
        .append_bytes("example.txt", b"Hello from Zstar\n")
        .await?;
    let output = builder.finish().await?;
    drop(output);
    // endregion create

    // region read
    let source = Arc::new(AsyncLocalFileSource::open("example.zstar").await?);
    let archive = Archive::open(source).await?;
    let content = archive.read("example.txt").await?;
    let selected = archive.read_range("example.txt", 0..5).await?;
    assert_eq!(content.as_ref(), b"Hello from Zstar\n");
    assert_eq!(selected.as_ref(), b"Hello");
    print!("{}", String::from_utf8_lossy(&content));
    println!("{}", String::from_utf8_lossy(&selected));
    // endregion read

    // region verify
    archive.verify(VerificationScope::Archive).await?;
    println!("Archive verified");
    // endregion verify
    Ok(())
}
