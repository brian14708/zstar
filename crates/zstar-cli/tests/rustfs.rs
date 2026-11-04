//! Exercise the CLI against the disposable service owned by just check-integration.
use std::{
    fs,
    path::Path,
    process::{Command, Output},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use zstar::s3::object_store::{
    ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjectPath, signer::Signer,
};

fn run(config: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_zstar"));
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("AWS_") {
            command.env_remove(key);
        }
    }
    command
        .env("AWS_CONFIG_FILE", config)
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            config.with_extension("credentials"),
        )
        .env("AWS_ACCESS_KEY_ID", "zstar-test")
        .env("AWS_SECRET_ACCESS_KEY", "zstar-test-secret")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_ALLOW_HTTP", "true")
        .args(args)
        .output()
        .unwrap()
}

fn success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires disposable local RustFS; run just check-integration"]
async fn profile_path_endpoint_and_conditional_multipart_roundtrip() {
    let endpoint = std::env::var("ZSTAR_S3_ENDPOINT").expect("local RustFS endpoint required");
    let parsed = reqwest::Url::parse(&endpoint).unwrap();
    assert_eq!(parsed.scheme(), "http");
    assert!(matches!(
        parsed.host_str(),
        Some("127.0.0.1" | "localhost" | "[::1]")
    ));
    let bucket = format!(
        "zstar-cli-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region("us-east-1")
        .with_endpoint(&endpoint)
        .with_allow_http(true)
        .with_access_key_id("zstar-test")
        .with_secret_access_key("zstar-test-secret")
        .build()
        .unwrap();
    let client = reqwest::Client::new();
    let url = store
        .signed_url(
            reqwest::Method::PUT,
            &ObjectPath::default(),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert!(client.put(url).send().await.unwrap().status().is_success());
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("config");
    fs::write(&config, format!("[default]\nregion = us-east-1\nservices = local\ns3 =\n  addressing_style = path\n[services local]\ns3 =\n  endpoint_url = {endpoint}\n")).unwrap();
    let input = root.path().join("input");
    fs::create_dir(&input).unwrap();
    let destination = format!("s3://{bucket}/archive.zstar");
    let key = ObjectPath::from("archive.zstar");
    // Incompressible deterministic data forces the CLI's multipart path.
    let mut state = 1_u32;
    let data: Vec<u8> = (0..9 << 20)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state.to_le_bytes()[0]
        })
        .collect();
    fs::write(input.join("data"), &data).unwrap();
    let source = input.to_str().unwrap();
    success(&run(&config, &["pack", source, &destination]));
    let original = store.get(&key).await.unwrap().bytes().await.unwrap();
    assert!(original.len() > 8 << 20);
    let duplicate = run(&config, &["pack", source, &destination]);
    assert!(!duplicate.status.success());
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("--force"));
    assert_eq!(
        store.get(&key).await.unwrap().bytes().await.unwrap(),
        original
    );
    success(&run(&config, &["inspect", &destination]));
    let restored = root.path().join("restored");
    success(&run(
        &config,
        &["extract", &destination, restored.to_str().unwrap()],
    ));
    assert_eq!(fs::read(restored.join("data")).unwrap(), data);
    fs::write(input.join("data"), b"replacement").unwrap();
    success(&run(&config, &["pack", source, &destination, "--force"]));
    assert!(store.get(&key).await.unwrap().bytes().await.unwrap().len() < 8 << 20);
    store.delete(&key).await.unwrap();
    let url = store
        .signed_url(
            reqwest::Method::DELETE,
            &ObjectPath::default(),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert!(
        client
            .delete(url)
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
}
