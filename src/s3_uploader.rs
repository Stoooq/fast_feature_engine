use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use std::path::Path;

pub async fn upload_to_s3(
    client: &Client,
    bucket_name: &str,
    file_path: &str,
    s3_key: &str,
) -> Result<(), aws_sdk_s3::Error> {
    let body = ByteStream::from_path(Path::new(file_path)).await;

    match body {
        Ok(b) => {
            client
                .put_object()
                .bucket(bucket_name)
                .key(s3_key)
                .body(b)
                .send()
                .await?;
            Ok(())
        }
        Err(e) => {
            panic!("Unable to load file for sending: {:?}", e);
        }
    }
}
