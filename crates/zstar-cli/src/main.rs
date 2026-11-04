mod remote;

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_runtime::env_config::{file::EnvConfigFiles, property::PropertiesKey};
use aws_types::os_shim_internal::{Env as AwsEnv, Fs as AwsFs};
use clap::{Parser, Subcommand};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
use zstar::ManifestOptions;
use zstar::backend::BlockingArchive;
use zstar::fs::{self as restore, LocalFileSource};
use zstar::s3::object_store::client::CredentialProvider;
use zstar::s3::object_store::{aws::AwsCredential, path::Path as ObjectPath};

#[expect(
    clippy::needless_pass_by_value,
    reason = "Ownership matches the public API or asynchronous boundary."
)]
fn s3_error(operation: &str, error: zstar::s3::object_store::Error) -> std::io::Error {
    use zstar::s3::object_store::Error as E;
    let message = match error {
        E::Generic {
            store: "AWS credentials",
            ..
        } => "AWS credentials unavailable; select a configured profile with AWS_PROFILE or configure the standard AWS credential chain".to_owned(),
        E::NotFound { .. } => format!("S3 {operation} failed: object not found"),
        E::PermissionDenied { .. } | E::Unauthenticated { .. } => format!(
            "S3 {operation} failed: authorization denied; check the selected AWS profile and bucket permissions"
        ),
        _ => format!(
            "S3 {operation} failed; check the AWS profile, bucket, region, endpoint, and network configuration"
        ),
    };
    std::io::Error::other(message)
}

#[derive(clap::Args, Clone)]
struct MetadataLimits {
    #[arg(long = "metadata-memory-bytes", default_value_t = 8 << 20)]
    memory: u64,
    #[arg(long = "max-metadata-bytes", default_value_t = 4 << 30)]
    max_size: u64,
}
impl MetadataLimits {
    fn options(&self) -> ManifestOptions {
        ManifestOptions {
            memory_bytes: self.memory,
            max_encoded_bytes: self.max_size,
            max_decoded_bytes: self.max_size,
            ..Default::default()
        }
    }
}
fn level(text: &str) -> Result<i32, String> {
    let value: i32 = text.parse().map_err(|e| format!("{e}"))?;
    if (-131_072..=22).contains(&value) {
        Ok(value)
    } else {
        Err("must be between -131072 and 22".into())
    }
}
fn worker_count(text: &str) -> Result<usize, String> {
    let value: usize = text.parse().map_err(|e| format!("{e}"))?;
    if (1..=16).contains(&value) {
        Ok(value)
    } else {
        Err("must be between 1 and 16".into())
    }
}
#[derive(clap::Args)]
struct EncodingOptions {
    /// Preserve filesystem modes, ownership and timestamps.
    #[arg(long)]
    preserve_metadata: bool,
    #[arg(long, default_value_t = 1 << 20)]
    frame_size: usize,
    /// Zstandard level, -131072 to 22.
    #[arg(
        long,
        default_value_t = 3,
        allow_hyphen_values = true,
        value_parser = level
    )]
    compression_level: i32,
    /// Compression worker threads, 1 to 16.
    #[arg(long, value_parser = worker_count)]
    workers: Option<usize>,
    #[arg(long, default_value_t = 64 << 10)]
    metadata_block_size: usize,
    #[command(flatten)]
    metadata: MetadataLimits,
}
impl EncodingOptions {
    fn options(&self) -> restore::PackOptions {
        restore::PackOptions {
            preserve_metadata: self.preserve_metadata,
            frame_size: self.frame_size,
            compression_level: self.compression_level,
            workers: self
                .workers
                .unwrap_or_else(|| restore::PackOptions::default().workers),
            manifest: ManifestOptions {
                block_target: self.metadata_block_size,
                ..self.metadata.options()
            },
        }
    }
}
#[derive(Parser)]
#[command(
    name = "zstar",
    about = "Pack, inspect and extract entry-oriented Zstar archives"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Pack {
        input: PathBuf,
        output: PathBuf,
        /// Replace an existing output file.
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        encoding: EncodingOptions,
    },
    Inspect {
        archive: PathBuf,
        #[command(flatten)]
        metadata: MetadataLimits,
    },
    /// Restore through captured directory handles, refusing destination collisions.
    Extract {
        archive: PathBuf,
        destination: PathBuf,
        #[arg(long)]
        objects: Option<PathBuf>,
        #[arg(long)]
        allow_special_bits: bool,
        /// Verify external-object SHA-256 in addition to availability and length checks.
        #[arg(long)]
        verify_external_hash: bool,
        #[arg(long, default_value_t = 64)]
        max_directory_handles: usize,
        #[command(flatten)]
        metadata: MetadataLimits,
    },
}
#[expect(
    clippy::or_fun_call,
    reason = "The fallback is a cheap value conversion with no allocation or I/O."
)]
fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}
fn s3_location(path: &Path) -> restore::AnyResult<Option<(String, ObjectPath)>> {
    let Some(text) = path.to_str() else {
        return Ok(None);
    };
    let Some(rest) = text.strip_prefix("s3://") else {
        return Ok(None);
    };
    let (bucket, key) = rest
        .split_once('/')
        .ok_or("S3 URL must be s3://bucket/key")?;
    if bucket.is_empty() || key.is_empty() {
        return Err("S3 URL must be s3://bucket/key".into());
    }
    Ok(Some((bucket.to_owned(), ObjectPath::from(key))))
}

#[derive(Debug)]
struct AwsCredentials(SharedCredentialsProvider);

#[async_trait::async_trait]
impl CredentialProvider for AwsCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> zstar::s3::object_store::Result<Arc<AwsCredential>> {
        let credential = self.0.provide_credentials().await.map_err(|error| {
            zstar::s3::object_store::Error::Generic {
                store: "AWS credentials",
                source: Box::new(std::io::Error::other(format!(
                    "AWS credential provider could not load credentials; select a configured profile with AWS_PROFILE ({error})"
                ))),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: credential.access_key_id().to_owned(),
            secret_key: credential.secret_access_key().to_owned(),
            token: credential.session_token().map(str::to_owned),
        }))
    }
}

async fn s3_store(bucket: &str) -> restore::AnyResult<Arc<zstar::s3::AmazonS3>> {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let profile_set = aws_config::profile::parser::load(
        &AwsFs::default(),
        &AwsEnv::default(),
        &EnvConfigFiles::default(),
        None,
    )
    .await?;
    let credentials = config
        .credentials_provider()
        .ok_or("AWS credential chain did not return a provider")?;
    // The CLI owns ambient AWS configuration explicitly; the core library's
    // S3 builder intentionally has no environment-discovery path.
    let mut builder = zstar::s3::object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_credentials(Arc::new(AwsCredentials(credentials)));
    if let Some(region) = config.region() {
        builder = builder.with_region(region.as_ref());
    }
    let selected_profile = profile_set.get_profile(profile_set.selected_profile());
    let service_name = selected_profile.and_then(|profile| profile.get("services"));
    let profile_endpoint = service_name
        .and_then(|service| {
            let key = PropertiesKey::builder()
                .section_key("services")
                .section_name(service)
                .property_name("s3")
                .sub_property_name("endpoint_url")
                .build()
                .ok()?;
            profile_set.other_sections().get(&key)
        })
        .map(String::as_str);
    let addressing_style = selected_profile
        .and_then(|profile| profile.get("s3"))
        .and_then(|settings| {
            settings.lines().find_map(|line| {
                let (key, value) = line.trim().split_once('=')?;
                (key.trim() == "addressing_style").then(|| value.trim())
            })
        });
    let virtual_hosted = match std::env::var("AWS_VIRTUAL_HOSTED_STYLE_REQUEST") {
        Ok(value) => value.parse::<bool>()?,
        Err(std::env::VarError::NotPresent) => addressing_style == Some("virtual"),
        Err(error) => return Err(error.into()),
    };
    builder = builder.with_virtual_hosted_style_request(virtual_hosted);
    if std::env::var_os("AWS_ENDPOINT_URL_S3").is_none()
        && let Some(endpoint) = profile_endpoint
    {
        let endpoint = endpoint_for_style(endpoint, bucket, virtual_hosted)?;
        builder = builder.with_endpoint(endpoint);
    } else if std::env::var_os("AWS_ENDPOINT_URL_S3").is_none()
        && std::env::var_os("AWS_ENDPOINT").is_none()
        && let Some(endpoint) = config.endpoint_url()
    {
        builder = builder.with_endpoint(endpoint_for_style(endpoint, bucket, virtual_hosted)?);
    }
    Ok(Arc::new(builder.build()?))
}

fn endpoint_for_style(
    endpoint: &str,
    bucket: &str,
    virtual_hosted: bool,
) -> restore::AnyResult<String> {
    if !virtual_hosted {
        return Ok(endpoint.to_owned());
    }
    let mut url = url::Url::parse(endpoint)?;
    let host = url
        .host_str()
        .ok_or("S3 endpoint URL must include a host")?;
    url.set_host(Some(&format!("{bucket}.{host}")))
        .map_err(|_| "could not apply virtual-hosted S3 addressing")?;
    Ok(url.to_string().trim_end_matches('/').to_owned())
}

async fn download(path: &Path) -> restore::AnyResult<tempfile::NamedTempFile> {
    let (bucket, key) = s3_location(path)?.ok_or("expected an s3://bucket/key URL")?;
    let store = s3_store(&bucket).await?;
    let mut file = tempfile::NamedTempFile::new()?;
    remote::download(store.as_ref(), &key, file.as_file_mut()).await?;
    Ok(file)
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep the ordered validation or protocol operation together."
)]
#[tokio::main]
async fn main() -> restore::AnyResult<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .try_init();
    match Args::parse().command {
        Command::Pack {
            input,
            output,
            force,
            encoding,
        } => {
            let input_root = fs::canonicalize(&input)?;
            let remote = s3_location(&output)?;
            // Resolve the output path itself (following a final symlink when it
            // exists) so a link into the input tree is also rejected.
            let resolved = if remote.is_some() {
                None
            } else {
                Some(match fs::canonicalize(&output) {
                    Ok(path) => path,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        let name = output.file_name().ok_or("pack output must name a file")?;
                        fs::canonicalize(parent(&output))?.join(name)
                    }
                    Err(error) => return Err(error.into()),
                })
            };
            if resolved
                .as_ref()
                .is_some_and(|path| path.starts_with(&input_root))
            {
                return Err("pack output must be outside the input directory".into());
            }
            if !force && remote.is_none() && fs::symlink_metadata(&output).is_ok() {
                return Err(format!(
                    "{} already exists; pass --force to replace it",
                    output.display()
                )
                .into());
            }
            // 0666 requests the usual umask-filtered mode, like any new file.
            let mut staging = tempfile::Builder::new();
            staging.prefix(".zstar-");
            #[cfg(unix)]
            staging.permissions(std::os::unix::fs::PermissionsExt::from_mode(0o666));
            let mut staged = if remote.is_some() {
                tempfile::NamedTempFile::new()?
            } else {
                staging.tempfile_in(parent(&output))?
            };
            restore::pack_to_with_options(&input, staged.as_file_mut(), encoding.options())?;
            if let Some((bucket, key)) = remote {
                let store = s3_store(&bucket).await?;
                remote::upload(store.as_ref(), &key, staged.as_file_mut(), force).await?;
            } else if force {
                staged.persist(&output).map_err(|e| e.error)?;
            } else {
                // Atomic no-clobber publish: fails if another writer won the race.
                staged.persist_noclobber(&output).map_err(|e| e.error)?;
            }
        }
        Command::Inspect { archive, metadata } => {
            let staged = if s3_location(&archive)?.is_some() {
                Some(download(&archive).await?)
            } else {
                None
            };
            let local = staged
                .as_ref()
                .map_or(archive.as_path(), tempfile::NamedTempFile::path);
            let source = LocalFileSource::open(local)?;
            let size = source.len();
            if size < zstar::FOOTER_SIZE as u64 {
                return Err(zstar::Error::NotZstar.into());
            }
            let footer =
                zstar::parse_footer(&source.read_range(size - zstar::FOOTER_SIZE as u64..size)?)?;
            let parsed = BlockingArchive::open(source, metadata.options())?;
            println!(
                "metadata_offset={} metadata_encoded_size={} metadata_decoded_size={} entries={}",
                footer.toc_offset,
                footer.toc_encoded_len,
                footer.toc_decoded_len,
                parsed.manifest().root().entry_count
            );
        }
        Command::Extract {
            archive,
            destination,
            objects,
            allow_special_bits,
            verify_external_hash,
            max_directory_handles,
            metadata,
        } => {
            let staged = if s3_location(&archive)?.is_some() {
                Some(download(&archive).await?)
            } else {
                None
            };
            let local_archive = staged
                .as_ref()
                .map_or(archive.as_path(), tempfile::NamedTempFile::path);
            let report = restore::extract_file_with_report(
                local_archive,
                &destination,
                objects
                    .as_deref()
                    .map(restore::LocalObjects::open)
                    .transpose()?,
                restore::ExtractOptions {
                    allow_special_bits,
                    verify_external: verify_external_hash,
                    max_directory_handles,
                    manifest: metadata.options(),
                },
            )?;
            for omitted in &report.omitted {
                eprintln!(
                    "note: {:?} not applied to {} entries (e.g. {})",
                    omitted.field,
                    omitted.count,
                    omitted.examples.join(", ")
                );
            }
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::endpoint_for_style;

    #[test]
    fn profile_endpoints_follow_the_selected_addressing_style() {
        for endpoint in [
            "http://127.0.0.1:9000",
            "http://[::1]:9000",
            "https://s3.example",
        ] {
            assert_eq!(
                endpoint_for_style(endpoint, "bucket", false).unwrap(),
                endpoint
            );
        }
        assert_eq!(
            endpoint_for_style("https://s3.example", "bucket", true).unwrap(),
            "https://bucket.s3.example"
        );
    }
}
