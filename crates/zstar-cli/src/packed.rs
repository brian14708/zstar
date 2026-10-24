use bytes::Bytes;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zstar::{
    chunking::Chunking,
    fs::AnyResult,
    manifest_io::{ManifestOptions, Spool},
    pack::{
        Catalog, CatalogLimits, Generation, LocalPackStore, PackStore, PackWriter,
        PackWriterOptions, PackedObjectResolver, ResolverOptions, S3PackStore, Snapshot,
        export_objects, externalize_archive, materialize_archive, publish_generation,
    },
};

enum Store {
    Local(LocalPackStore),
    S3(S3PackStore),
}
impl PackStore for Store {
    fn cache_allowed(&self, snapshot: &Snapshot) -> bool {
        match self {
            Self::Local(store) => store.cache_allowed(snapshot),
            Self::S3(store) => store.cache_allowed(snapshot),
        }
    }
    async fn put(&self, key: String, data: Spool) -> zstar::Result<Snapshot> {
        match self {
            Self::Local(s) => s.put(key, data).await,
            Self::S3(s) => s.put(key, data).await,
        }
    }
    async fn get_range(&self, s: &Snapshot, r: std::ops::Range<u64>) -> zstar::Result<Bytes> {
        match self {
            Self::Local(store) => store.get_range(s, r).await,
            Self::S3(store) => store.get_range(s, r).await,
        }
    }
}
async fn store(path: &Path) -> AnyResult<Arc<Store>> {
    Ok(Arc::new(
        if let Some((bucket, root)) = super::s3_location(path)? {
            Store::S3(S3PackStore::new(
                super::s3_store(&bucket).await?,
                root,
                8 << 20,
                4,
            )?)
        } else {
            Store::Local(LocalPackStore::open(path)?)
        },
    ))
}
async fn spool(path: &Path) -> AnyResult<Spool> {
    let mut input = tokio::fs::File::open(path).await?;
    let mut spool = Spool::new(ManifestOptions::default());
    let mut buffer = vec![0; 64 << 10];
    loop {
        let n = input.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        let data = buffer[..n].to_vec();
        spool = tokio::task::spawn_blocking(move || {
            spool.append(&data)?;
            Ok::<_, zstar::Error>(spool)
        })
        .await??;
    }
    Ok(spool)
}
fn vacant(path: &Path) -> AnyResult<()> {
    if std::fs::symlink_metadata(path).is_ok() {
        return Err(format!("{} already exists", path.display()).into());
    }
    Ok(())
}
async fn persist(staged: tempfile::NamedTempFile, path: PathBuf) -> AnyResult<()> {
    tokio::task::spawn_blocking(move || {
        staged
            .persist_noclobber(path)
            .map(|_| ())
            .map_err(|e| e.error)
    })
    .await??;
    Ok(())
}
pub(super) async fn capture(
    input: PathBuf,
    output: PathBuf,
    catalog_output: PathBuf,
    root: PathBuf,
    fastcdc: bool,
) -> AnyResult<()> {
    vacant(&output)?;
    vacant(&catalog_output)?;
    if output == catalog_output {
        return Err("archive and catalog outputs must differ".into());
    }
    let input_root = std::fs::canonicalize(&input)?;
    for path in [&output, &catalog_output, &root] {
        if super::s3_location(path)?.is_none() {
            let resolved = if path.exists() {
                std::fs::canonicalize(path)?
            } else {
                std::fs::canonicalize(super::parent(path))?
                    .join(path.file_name().ok_or("output filename required")?)
            };
            if resolved.starts_with(&input_root) {
                return Err("packed outputs and store must be outside the input tree".into());
            }
        }
    }
    let portable = tokio::task::spawn_blocking(move || -> AnyResult<_> {
        let mut staged = tempfile::NamedTempFile::new()?;
        zstar::fs::pack_to_with_options(&input, staged.as_file_mut(), Default::default())?;
        Ok(staged)
    })
    .await??;
    let source = Arc::new(zstar::fs::AsyncLocalFileSource::open(portable.path()).await?);
    let archive = zstar::Archive::open(source).await?;
    let store = store(&root).await?;
    let strategy = if fastcdc {
        Chunking::fastcdc()
    } else {
        Chunking::default()
    };
    let mut packs = PackWriter::new(
        store.clone(),
        Catalog::default(),
        PackWriterOptions::default(),
    )?;
    let staged = tempfile::NamedTempFile::new_in(super::parent(&output))?;
    let sink = tokio::fs::File::from_std(staged.reopen()?);
    if let Err(error) =
        externalize_archive(&archive, sink, strategy, &mut packs, Default::default()).await
    {
        eprintln!("unpublished packs: {:?}", packs.completed_packs());
        return Err(error.into());
    }
    let catalog = packs.finish().await?;
    let encoded = catalog.encode(CatalogLimits::default())?;
    let catalog_stage = tempfile::NamedTempFile::new_in(super::parent(&catalog_output))?;
    let mut sink = tokio::fs::File::from_std(catalog_stage.reopen()?);
    sink.write_all(&encoded).await?;
    sink.sync_all().await?;
    drop(sink);
    let publication = spool(staged.path()).await?;
    persist(staged, output).await?;
    persist(catalog_stage, catalog_output).await?;
    match publish_generation(
        store.as_ref(),
        "objects",
        publication,
        &catalog,
        strategy,
        CatalogLimits::default(),
    )
    .await
    {
        Ok((descriptor, _)) => println!("{}", descriptor.key),
        Err(error) => {
            eprintln!(
                "unpublished packs: {:?}; artifacts: {:?}",
                packs.completed_packs(),
                error.completed
            );
            return Err(error.into());
        }
    }
    Ok(())
}
async fn catalog(path: PathBuf) -> AnyResult<Catalog> {
    let metadata = tokio::fs::metadata(&path).await?;
    if metadata.len() > 16 << 20 {
        return Err("catalog encoded-byte limit".into());
    }
    let data = tokio::fs::read(path).await?;
    Ok(Catalog::decode(&data, CatalogLimits::default())?)
}
pub(super) async fn materialize(
    input: PathBuf,
    catalog_path: PathBuf,
    output: PathBuf,
    root: PathBuf,
) -> AnyResult<()> {
    vacant(&output)?;
    let store = store(&root).await?;
    let resolver = Arc::new(PackedObjectResolver::new(
        store,
        catalog(catalog_path).await?,
        ResolverOptions::default(),
    )?);
    let source = Arc::new(zstar::fs::AsyncLocalFileSource::open(input).await?);
    write_portable(source, resolver, output).await
}
async fn write_portable(
    source: Arc<zstar::fs::AsyncLocalFileSource>,
    resolver: Arc<dyn zstar::ObjectResolver>,
    output: PathBuf,
) -> AnyResult<()> {
    let archive = zstar::Archive::open_with_options(
        source,
        zstar::ReaderOptions {
            resolver: Some(resolver),
            ..Default::default()
        },
    )
    .await?;
    let staged = tempfile::NamedTempFile::new_in(super::parent(&output))?;
    let sink = tokio::fs::File::from_std(staged.reopen()?);
    materialize_archive(&archive, sink, Default::default())
        .await?
        .sync_all()
        .await?;
    persist(staged, output).await
}
pub(super) async fn generation(key: String, output: PathBuf, root: PathBuf) -> AnyResult<()> {
    vacant(&output)?;
    let store = store(&root).await?;
    // The descriptor itself must be supplied explicitly. Local roots load its
    // immutable bytes; S3 loads through a pinned native source before interpretation.
    let data = if let Some((bucket, prefix)) = super::s3_location(&root)? {
        let s = zstar::s3::S3RangeSource::new(
            super::s3_store(&bucket).await?,
            zstar::s3::Path::from(format!("{prefix}/{key}")),
        );
        use zstar::AsyncRangeReader;
        let size = s.size().await?;
        if size > 64 << 10 {
            return Err("generation descriptor limit".into());
        }
        s.read_range(0..size).await?
    } else {
        if !zstar::manifest::valid_path(&key) {
            return Err("invalid generation key".into());
        }
        let path = root.join(&key);
        let length = tokio::fs::metadata(&path).await?.len();
        if length > 64 << 10 {
            return Err("generation descriptor limit".into());
        }
        Bytes::from(tokio::fs::read(path).await?)
    };
    let g = Generation::decode(&data)?;
    if g.catalog.length > 16 << 20 {
        return Err("catalog encoded-byte limit".into());
    }
    let encoded = store.get_range(&g.catalog, 0..g.catalog.length).await?;
    let catalog = Catalog::decode(&encoded, CatalogLimits::default())?;
    let staged = tempfile::NamedTempFile::new()?;
    let mut sink = tokio::fs::File::from_std(staged.reopen()?);
    let mut offset = 0;
    while offset < g.archive.length {
        let end = (offset + (8 << 20)).min(g.archive.length);
        sink.write_all(&store.get_range(&g.archive, offset..end).await?)
            .await?;
        offset = end;
    }
    sink.flush().await?;
    drop(sink);
    let resolver = Arc::new(PackedObjectResolver::new(
        store,
        catalog,
        ResolverOptions::default(),
    )?);
    write_portable(
        Arc::new(zstar::fs::AsyncLocalFileSource::open(staged.path()).await?),
        resolver,
        output,
    )
    .await
}
pub(super) async fn export(
    catalog_path: PathBuf,
    root: PathBuf,
    destination: PathBuf,
) -> AnyResult<()> {
    let store = store(&root).await?;
    let catalog = catalog(catalog_path).await?;
    let resolver = PackedObjectResolver::new(store, catalog.clone(), ResolverOptions::default())?;
    let target = LocalPackStore::open(destination)?;
    export_objects(&catalog, &resolver, &target, "objects").await?;
    Ok(())
}
