//! Run with `cargo run -p zstar --no-default-features --features fs --example blocking_file -- archive.zstar`.
use zstar::blocking::Archive;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or("expected an archive path")?;
    let archive = Archive::open_path(path)?;
    for entry in archive.entries() {
        let entry = entry?;
        println!("{}\t{}", entry.size(), entry.path());
    }
    Ok(())
}
