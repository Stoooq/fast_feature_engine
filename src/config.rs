use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Settings {
    pub app: AppConfig,
    pub offline: OfflineConfig,
    pub live: LiveConfig,
    pub aws: AwsConfig,
}

#[derive(Deserialize, Debug)]
pub struct AppConfig {
    pub mode: String,
    pub batch_size: usize,
    pub tail_size: usize,
    pub output_path: String,
}

#[derive(Deserialize, Debug)]
pub struct OfflineConfig {
    pub directory_path: String,
}

#[derive(Deserialize, Debug)]
pub struct LiveConfig {
    pub websocket_url: String,
}

#[derive(Deserialize, Debug)]
pub struct AwsConfig {
    pub bucket_name: String,
    pub upload_to_s3: bool,
}