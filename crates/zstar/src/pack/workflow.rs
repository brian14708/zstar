use super::{Catalog, PackStore, Snapshot};
use crate::chunking::{Chunking, ObjectWriter, external_file};
use crate::manifest_io::{ManifestOptions, Spool};
use crate::{
    Archive, AsyncRangeReader, Builder, Entry, EntryKind, Error, Header, ObjectResolver, Result,
    WriterOptions,
};
use tokio::io::AsyncWrite;

fn header(entry: &Entry, size: u64) -> Header {
    let mut header = match entry.kind {
        EntryKind::File => Header::file(&entry.path, size),
        EntryKind::Directory => Header::directory(&entry.path),
        EntryKind::Symlink => {
            Header::symlink(&entry.path, entry.linkname.as_deref().unwrap_or_default())
        }
    };
    header.set_mode(entry.mode);
    header.set_uid(entry.uid);
    header.set_gid(entry.gid);
    header.set_mtime_ns(entry.mtime_ns);
    header.set_attributes(entry.attributes.clone());
    header
}
/// Rewrite logical members into explicit external objects. Files reset chunking;
/// metadata and root extensions survive. The caller finalizes the object writer
/// before publishing the resulting archive/catalog generation.
pub async fn externalize_archive<R, W, O>(
    archive: &Archive<R>,
    output: W,
    strategy: Chunking,
    objects: &mut O,
    mut options: WriterOptions,
) -> Result<W>
where
    R: AsyncRangeReader + ?Sized + 'static,
    W: AsyncWrite + Unpin,
    O: ObjectWriter,
{
    strategy.validate()?;
    archive.verify(crate::VerificationScope::Archive).await?;
    options.root_extensions = archive.root_extensions().clone();
    let mut builder = Builder::with_options(output, options);
    let mut cursor = archive.cursor().await?;
    loop {
        let (next, entries) = archive.next_entries(cursor, 64).await?;
        cursor = next;
        if entries.is_empty() {
            break;
        }
        for mut entry in entries {
            let size = entry.size()?;
            if entry.kind == EntryKind::File && size > 0 {
                let mut body = archive.open_file(&entry.path).await?;
                entry.extents = external_file(&mut body, size, strategy, objects).await?;
                builder.append_external(entry).await?;
            } else {
                builder.append_header(header(&entry, 0)).await?;
            }
        }
    }
    builder.finish().await
}
/// Reconstruct an ordinary portable v1 archive using the configured resolver.
pub async fn materialize_archive<R, W>(
    archive: &Archive<R>,
    output: W,
    mut options: WriterOptions,
) -> Result<W>
where
    R: AsyncRangeReader + ?Sized + 'static,
    W: AsyncWrite + Unpin,
{
    archive.verify(crate::VerificationScope::Archive).await?;
    options.root_extensions = archive.root_extensions().clone();
    let mut builder = Builder::with_options(output, options);
    let mut cursor = archive.cursor().await?;
    loop {
        let (next, entries) = archive.next_entries(cursor, 64).await?;
        cursor = next;
        if entries.is_empty() {
            break;
        }
        for entry in entries {
            let size = entry.size()?;
            if entry.kind == EntryKind::File && size > 0 {
                let mut body = archive.open_file(&entry.path).await?;
                builder.append_data(header(&entry, size), &mut body).await?;
            } else {
                builder.append_header(header(&entry, 0)).await?;
            }
        }
    }
    builder.finish().await
}
/// Export complete verified catalog frames to the standalone `<digest>.zst` layout.
/// Completed keys remain caller-owned even if a later object fails.
pub async fn export_objects<S: PackStore>(
    catalog: &Catalog,
    resolver: &dyn ObjectResolver,
    store: &S,
    prefix: &str,
) -> Result<Vec<Snapshot>> {
    if !crate::manifest::valid_path(prefix) {
        return Err(Error::Unsupported("invalid export prefix"));
    }
    let mut completed = Vec::new();
    for chunk in catalog.chunks().values() {
        let encoded = resolver
            .get_encoded(chunk.digest, chunk.encoded_length, 17 << 20)
            .await?;
        let raw = crate::decode_frame(&encoded, chunk.decoded_length as usize)?;
        crate::verify_sha256(&raw, chunk.digest)?;
        let address = crate::manifest::object_address(&chunk.digest);
        let mut spool = Spool::new(ManifestOptions::default());
        spool.append(&encoded)?;
        let snapshot = store
            .put(format!("{prefix}/{}.zst", &address[7..]), spool)
            .await?;
        completed.push(snapshot);
    }
    Ok(completed)
}
