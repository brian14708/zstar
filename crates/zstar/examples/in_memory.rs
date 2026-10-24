//! Create an archive, read a range, and seek through a retained file handle.
use std::io::SeekFrom;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use zstar::{Archive, Builder, Header, VerificationScope};

#[tokio::main]
async fn main() -> zstar::Result<()> {
    let mut builder = Builder::new(Vec::new());
    builder
        .append_data(Header::file("hello.txt", 5), b"hello".as_slice())
        .await?;
    let archive = Archive::from_bytes(builder.finish().await?).await?;
    assert_eq!(
        archive.read_range("hello.txt", 1..4).await?,
        b"ell".as_slice()
    );

    let mut file = archive.open_file("hello.txt").await?;
    file.seek(SeekFrom::Start(2)).await?;
    let mut tail = String::new();
    file.read_to_string(&mut tail).await?;
    println!("Read after seeking: {tail}");
    archive.verify(VerificationScope::Archive).await?;
    Ok(())
}
