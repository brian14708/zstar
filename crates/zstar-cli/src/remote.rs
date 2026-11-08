//! Bounded S3 transfers and atomic publication of completed archives.

use super::AnyResult;
use futures_util::TryStreamExt;
use std::{
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    sync::Arc,
};
use zstar::s3::object_store::{ObjectStoreExt, path::Path};
use zstar::s3::{AmazonS3, ArchiveUpload, UploadMode};

#[cfg(test)]
mod tests;

pub async fn download(store: &AmazonS3, key: &Path, file: &mut File) -> AnyResult<()> {
    let mut stream = store
        .get(key)
        .await
        .map_err(|error| super::s3_error("GET", error))?
        .into_stream();
    while let Some(bytes) = stream
        .try_next()
        .await
        .map_err(|error| super::s3_error("GET body", error))?
    {
        file.write_all(&bytes)?;
    }
    Ok(())
}

fn publication_error(error: zstar::Error) -> std::io::Error {
    let zstar::Error::Io(error) = error else {
        return std::io::Error::other(format!("S3 upload failed: {error}"));
    };
    match error.kind() {
        ErrorKind::AlreadyExists => std::io::Error::new(
            ErrorKind::AlreadyExists,
            "S3 output already exists; pass --force to replace it",
        ),
        ErrorKind::PermissionDenied => std::io::Error::other(
            "S3 upload failed: authorization denied; check the selected AWS profile and bucket permissions",
        ),
        _ => std::io::Error::other(format!(
            "S3 upload failed: {error}; check the AWS profile, bucket, region, endpoint, and network configuration"
        )),
    }
}

pub async fn upload(
    store: &Arc<AmazonS3>,
    key: &Path,
    file: &mut File,
    force: bool,
) -> AnyResult<()> {
    let size = file.metadata()?.len();
    let mode = if force {
        UploadMode::Overwrite
    } else {
        UploadMode::Create
    };
    let mut upload = ArchiveUpload::new(store.clone(), key.clone(), mode).with_size_hint(size)?;
    file.seek(SeekFrom::Start(0))?;
    let mut buffer = vec![0; 1 << 20];
    let written = async {
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                return Ok(());
            }
            upload
                .write(&buffer[..count])
                .await
                .map_err(publication_error)?;
        }
    }
    .await;
    if let Err(error) = written {
        let _ = upload.abort().await;
        return Err(error);
    }
    // finish aborts a failed multipart publication itself.
    Ok(upload.finish().await.map_err(publication_error)?)
}
