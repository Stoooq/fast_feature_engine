pub mod batch;
pub mod config;
pub mod features;
pub mod models;
pub mod reader;
pub mod s3_uploader;

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use batch::DataBatch;
use config::Settings;
use features::{FeatureGenerator, LogReturnGenerator, RollingVolatility, RollingVwap};
use futures_util::StreamExt;
use models::BinanceTrade;
use polars::prelude::*;
use reader::BatchReader;
use s3_uploader::upload_to_s3;
use std::fs;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let config_contents = fs::read_to_string("config.toml").unwrap();
    let config: Arc<Settings> = Arc::new(toml::from_str(&config_contents).unwrap());

    println!("Running in config mode: {}", config.app.mode.to_uppercase());

    let s3_client = if config.aws.upload_to_s3 {
        let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        Some(Client::new(&aws_config))
    } else {
        None
    };

    let feature_generators: Arc<Vec<Box<dyn FeatureGenerator + Send + Sync>>> = Arc::new(vec![
        Box::new(LogReturnGenerator {
            intervals_ms: vec![None, Some(10_000), Some(60_000), Some(600_000)],
        }),
        Box::new(RollingVolatility {
            intervals_ms: vec![10_000, 60_000, 600_000],
        }),
        Box::new(RollingVwap {
            intervals_ms: vec![10_000, 60_000, 600_000],
        }),
    ]);

    let generators_clone = Arc::clone(&feature_generators);

    match config.app.mode.as_str() {
        "offline" => {
            run_offline_engine(config, s3_client, generators_clone).await;
        }
        "live" => {
            run_live_engine(config, s3_client, generators_clone).await;
        }
        _ => {
            eprintln!(
                "Unknown mode of operation in config.toml: {}. Use 'offline' or 'live'.",
                config.app.mode
            );
        }
    }

    Ok(())
}

async fn run_offline_engine(
    config: Arc<Settings>,
    s3_client: Option<aws_sdk_s3::Client>,
    feature_generators: Arc<Vec<Box<dyn FeatureGenerator + Send + Sync>>>,
) {
    println!(
        "Start reading from the catalog: {}",
        config.offline.directory_path
    );

    let reader = BatchReader::new(
        &config.offline.directory_path,
        config.app.batch_size,
        config.app.tail_size,
    );

    for (batch_id, current_batch) in reader.enumerate() {
        let generators_clone = Arc::clone(&feature_generators);
        let config_clone = Arc::clone(&config);

        generate_features(
            config_clone,
            current_batch,
            batch_id,
            generators_clone,
            s3_client.clone(),
        )
        .await;
    }
}

async fn run_live_engine(
    config: Arc<Settings>,
    s3_client: Option<aws_sdk_s3::Client>,
    feature_generators: Arc<Vec<Box<dyn FeatureGenerator + Send + Sync>>>,
) {
    println!("Connecting to WebSocket: {}", config.live.websocket_url);

    let (ws_stream, _) = connect_async(config.live.websocket_url.clone())
        .await
        .unwrap();

    let (_, mut read) = ws_stream.split();

    let mut trade_buffer: Vec<BinanceTrade> = Vec::with_capacity(config.app.batch_size);
    let mut tail_buffer: Vec<BinanceTrade> = Vec::with_capacity(config.app.tail_size);
    let mut batch_counter = 0;

    while let Some(msg) = read.next().await {
        let msg = msg.unwrap();

        if let Message::Text(text) = msg {
            if let Ok(trade) = serde_json::from_str::<BinanceTrade>(&text) {
                trade_buffer.push(trade);
                println!("{}", trade_buffer.len());

                if trade_buffer.len() >= config.app.batch_size {
                    println!("New batch nr {}", batch_counter);

                    let current_new_trades = trade_buffer.clone();
                    
                    let mut buffer_to_process = tail_buffer.clone();
                    buffer_to_process.extend(current_new_trades.clone());

                    let tail_start_idx = current_new_trades.len().saturating_sub(config.app.tail_size);
                    tail_buffer = current_new_trades[tail_start_idx..].to_vec();

                    trade_buffer.clear();

                    let generators_clone = Arc::clone(&feature_generators);
                    let config_clone = Arc::clone(&config);
                    let s3_client_clone = s3_client.clone();

                    tokio::spawn(async move {
                        let prices: Vec<f64> = buffer_to_process
                            .iter()
                            .map(|x| x.price.parse().unwrap())
                            .collect();
                        let volumes: Vec<f64> = buffer_to_process
                            .iter()
                            .map(|x| x.volume.parse().unwrap())
                            .collect();
                        let times: Vec<i64> = buffer_to_process.iter().map(|x| x.time).collect();

                        let df = DataFrame::new(
                            times.len(),
                            vec![
                                Column::new("price".into(), prices),
                                Column::new("qty".into(), volumes),
                                Column::new("time".into(), times),
                            ],
                        )
                        .unwrap();

                        generate_features(
                            config_clone,
                            df,
                            batch_counter,
                            generators_clone,
                            s3_client_clone,
                        )
                        .await;
                    });

                    batch_counter += 1;
                }
            }
        }
    }
}

async fn generate_features(
    config: Arc<Settings>,
    current_batch: DataFrame,
    batch_id: usize,
    feature_generators: Arc<Vec<Box<dyn FeatureGenerator + Send + Sync>>>,
    s3_client: Option<aws_sdk_s3::Client>,
) {
    let config_clone = Arc::clone(&config);

    let part_file_name = tokio::task::spawn_blocking(move || {
        let mut batch = DataBatch { df: current_batch };
        let now = Instant::now();

        for generator in feature_generators.iter() {
            let new_columns = generator.generate(&batch).unwrap();
            batch.df = batch.df.hstack(&new_columns).unwrap();
        }

        if batch_id > 0 {
            batch.df = batch.df.slice(
                config_clone.app.tail_size as i64,
                batch.df.height() - config_clone.app.tail_size,
            );
        }

        println!("Batch {}: {}", batch_id, now.elapsed().as_micros());

        let target_dir = format!(
            "{}dataset_{}",
            config_clone.app.output_path,
            chrono::Local::now().format("%Y%m%d")
        );
        let part_file_name_local = format!("{}/part_{:04}.parquet", target_dir, batch_id);
        std::fs::create_dir_all(&target_dir).unwrap();
        let mut file = File::create(&part_file_name_local).unwrap();
        ParquetWriter::new(&mut file).finish(&mut batch.df).unwrap();

        part_file_name_local
    })
    .await
    .unwrap();

    if let Some(client) = &s3_client {
        let s3_dataset_dir = format!("dataset_{}", chrono::Local::now().format("%Y%m%d"));
        let s3_key = format!("{}/part_{:04}.parquet", s3_dataset_dir, batch_id);
        upload_to_s3(
            client,
            config.aws.bucket_name.as_str(),
            &part_file_name,
            &s3_key,
        )
        .await
        .unwrap();
    }
}