//! Create an archive, upload it to S3, read it, and delete it.
use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use zstar::s3::object_store::{ObjectStore, ObjectStoreExt};
use zstar::s3::{AmazonS3Builder, Path, S3RangeSource, put_archive};
use zstar::{Archive, Builder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // region configure
    let mut config = AmazonS3Builder::new()
        .with_bucket_name(env::var("ZSTAR_S3_BUCKET")?)
        .with_region(env::var("AWS_REGION")?)
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY")?);
    if let Ok(token) = env::var("AWS_SESSION_TOKEN") {
        config = config.with_token(token);
    }
    if let Ok(endpoint) = env::var("ZSTAR_S3_ENDPOINT") {
        let allow_http = endpoint.starts_with("http://");
        config = config
            .with_endpoint(endpoint)
            .with_allow_http(allow_http)
            .with_virtual_hosted_style_request(false);
    }
    let store: Arc<dyn ObjectStore> = Arc::new(config.build()?);
    // endregion configure

    // region upload
    let mut builder = Builder::new(Vec::new());
    builder
        .append_bytes("example.txt", b"Hello from Zstar\n")
        .await?;
    let encoded = builder.finish().await?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = Path::from(format!(
        "zstar-tutorial/rust-{}-{nonce}.zstar",
        std::process::id()
    ));
    put_archive(store.as_ref(), &path, encoded.into()).await?;
    // endregion upload

    // region cleanup
    let read_result = read_archive(Arc::clone(&store), path.clone()).await;
    let cleanup_result = store.delete(&path).await;
    read_result?;
    cleanup_result?;
    println!("Deleted S3 object");
    // endregion cleanup
    Ok(())
}

async fn read_archive(store: Arc<dyn ObjectStore>, path: Path) -> zstar::Result<()> {
    // region read
    let source = Arc::new(S3RangeSource::new(store, path));
    let archive = Archive::open(source).await?;
    let content = archive.read("example.txt").await?;
    let selected = archive.read_range("example.txt", 0..5).await?;
    if content.as_ref() != b"Hello from Zstar\n" || selected.as_ref() != b"Hello" {
        return Err(std::io::Error::other("unexpected archive contents").into());
    }
    print!("{}", String::from_utf8_lossy(&content));
    println!("{}", String::from_utf8_lossy(&selected));
    // endregion read
    Ok(())
}
