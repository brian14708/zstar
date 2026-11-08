//! Run with `cargo run -p zstar --no-default-features --example blocking_memory`.
use std::io::{Read, Seek, SeekFrom};
use zstar::blocking::{Archive, Builder};

fn main() -> zstar::Result<()> {
    let mut writer = Builder::try_new(Vec::new())?;
    writer.append_dir("data")?;
    writer.append_bytes("data/hello.txt", b"hello world")?;
    let mut archive = Archive::from_bytes(writer.finish()?)?;
    assert_eq!(archive.read_range("data/hello.txt", 6..11)?, b"world");
    for entry in archive.entries() {
        println!("{}", entry?.path());
    }
    let mut member = archive.open_file("data/hello.txt")?;
    member.seek(SeekFrom::Start(6))?;
    let mut text = String::new();
    member.read_to_string(&mut text)?;
    assert_eq!(text, "world");
    Ok(())
}
