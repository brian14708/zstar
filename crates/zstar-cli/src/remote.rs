//! Bounded S3 transfers and atomic publication of completed archives.

use futures_util::TryStreamExt;
use reqwest::{Client, Method, StatusCode, header};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    time::Duration,
};
use zstar::fs::AnyResult;
use zstar::s3::AmazonS3;
use zstar::s3::object_store::{
    ObjectStore, ObjectStoreExt, PutMode,
    multipart::{MultipartStore, PartId},
    path::Path,
    signer::{SignedUrlOptions, Signer},
};

const PART_BYTES: u64 = 8 << 20;
const MAX_OBJECT_BYTES: u64 = 5 << 40;
const MAX_PARTS: u64 = 10_000;

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

fn read_part(file: &mut File, limit: u64) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    file.take(limit).read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn publication_error(error: zstar::s3::object_store::Error) -> std::io::Error {
    if matches!(
        error,
        zstar::s3::object_store::Error::AlreadyExists { .. }
            | zstar::s3::object_store::Error::Precondition { .. }
    ) {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "S3 output already exists; pass --force to replace it",
        )
    } else {
        super::s3_error("upload", error)
    }
}

pub async fn upload(store: &AmazonS3, key: &Path, file: &mut File, force: bool) -> AnyResult<()> {
    let size = file.metadata()?.len();
    if size > MAX_OBJECT_BYTES {
        return Err("archive exceeds the S3 object size limit".into());
    }
    file.seek(SeekFrom::Start(0))?;
    if size <= PART_BYTES {
        let mode = if force {
            PutMode::Overwrite
        } else {
            PutMode::Create
        };
        store
            .put_opts(key, read_part(file, PART_BYTES)?.into(), mode.into())
            .await
            .map_err(publication_error)?;
        return Ok(());
    }

    let id = store
        .create_multipart(key)
        .await
        .map_err(|error| super::s3_error("start upload", error))?;
    let result: AnyResult<()> = async {
        // Stay within S3's part-count limit even for multi-terabyte archives.
        let part_size = PART_BYTES.max(size.div_ceil(MAX_PARTS));
        let mut parts = Vec::new();
        while let bytes = read_part(file, part_size)?
            && !bytes.is_empty()
        {
            parts.push(
                store
                    .put_part(key, &id, parts.len(), bytes.into())
                    .await
                    .map_err(|error| super::s3_error("upload part", error))?,
            );
        }
        if force {
            store
                .complete_multipart(key, &id, parts)
                .await
                .map_err(publication_error)?;
        } else {
            complete_create(store, key, &id, &parts).await?;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        // Preserve the original failure, but release uploaded parts on failure.
        let _ = store.abort_multipart(key, &id).await;
    }
    result
}

#[derive(Serialize)]
#[serde(rename = "CompleteMultipartUpload")]
struct Completion<'a> {
    #[serde(rename = "Part")]
    parts: Vec<CompletionPart<'a>>,
}

#[derive(Serialize)]
struct CompletionPart<'a> {
    #[serde(rename = "PartNumber")]
    number: usize,
    #[serde(rename = "ETag")]
    etag: &'a str,
}

#[derive(Deserialize)]
struct Completed {
    #[serde(rename = "ETag")]
    etag: String,
}

async fn complete_create(
    store: &AmazonS3,
    key: &Path,
    id: &str,
    parts: &[PartId],
) -> AnyResult<()> {
    // object_store's multipart completion always overwrites. Sign a conditional
    // completion instead, so a concurrent writer cannot be silently replaced.
    let options = SignedUrlOptions::default()
        .with_query([("uploadId", id)])
        .with_signed_header(header::IF_NONE_MATCH, header::HeaderValue::from_static("*"));
    let url = store
        .signed_url_opts(Method::POST, key, Duration::from_secs(300), &options)
        .await
        .map_err(|error| super::s3_error("sign upload completion", error))?;
    let body = quick_xml::se::to_string(&Completion {
        parts: parts
            .iter()
            .enumerate()
            .map(|(index, part)| CompletionPart {
                number: index + 1,
                etag: &part.content_id,
            })
            .collect(),
    })?;
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(300))
        .build()?;
    let mut response = client
        .post(url)
        .header(header::IF_NONE_MATCH, "*")
        .header(header::CONTENT_TYPE, "application/xml")
        .body(body)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;
    if response.status() == StatusCode::PRECONDITION_FAILED {
        return Err("S3 output already exists; pass --force to replace it".into());
    }
    response = response
        .error_for_status()
        .map_err(reqwest::Error::without_url)?;
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        if body.len() + chunk.len() > 64 << 10 {
            return Err("S3 upload completion response exceeds 64 KiB".into());
        }
        body.extend_from_slice(&chunk);
    }
    // S3 can return HTTP 200 with an embedded XML error. Require a completed
    // object's ETag rather than treating the HTTP status alone as success.
    let completed: Completed = quick_xml::de::from_reader(body.as_slice())
        .map_err(|_| "S3 upload completion did not return an object ETag")?;
    if completed.etag.is_empty() {
        return Err("S3 upload completion returned an empty ETag".into());
    }
    Ok(())
}
