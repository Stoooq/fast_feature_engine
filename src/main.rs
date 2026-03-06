pub mod batch;
pub mod features;
pub mod models;
pub mod reader;
pub mod s3_uploader;

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use batch::DataBatch;
use features::{FeatureGenerator, LogReturnGenerator, RollingVolatility, RollingVwap};
use futures_util::StreamExt;
use models::BinanceTrade;
use polars::prelude::*;
use reader::BatchReader;
use s3_uploader::upload_to_s3;
use std::fs::File;
use std::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let batch_size = 10_000;
    let tail_size = 500;
    let directory_path = "data/";
    let output_path = "output/";

    dotenvy::dotenv().ok();

    let aws_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = Client::new(&aws_config);
    let bucket_name = std::env::var("S3_BUCKET_NAME").unwrap();

    let url = "wss://stream.binance.com:9443/ws/btcusdt@trade";
    println!("Connecting with Binance");

    let (ws_stream, _) = connect_async(url).await.unwrap();
    println!("Connected");

    let (_, mut read) = ws_stream.split();

    let mut trade_buffer: Vec<BinanceTrade> = Vec::with_capacity(batch_size);
    let mut batch_counter = 0;

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

    while let Some(msg) = read.next().await {
        let msg = msg.unwrap();

        if let Message::Text(text) = msg {
            if let Ok(trade) = serde_json::from_str::<BinanceTrade>(&text) {
                trade_buffer.push(trade);
                println!("{}", trade_buffer.len());

                if trade_buffer.len() >= batch_size {
                    println!("New batch nr {}", batch_counter);

                    let buffer_to_process = trade_buffer.clone();
                    trade_buffer.clear();

                    let s3_client_clone = s3_client.clone();
                    let bucket_clone = bucket_name.to_string();
                    let out_path = output_path.to_string();
                    let generators_clone = Arc::clone(&feature_generators);

                    tokio::spawn(async move {
                        process_and_upload_batch(
                            buffer_to_process,
                            batch_counter,
                            out_path,
                            s3_client_clone,
                            bucket_clone,
                            generators_clone,
                        )
                        .await;
                    });

                    batch_counter += 1;
                }
            }
        }
    }

    Ok(())
}

async fn process_and_upload_batch(
    raw_trades: Vec<BinanceTrade>,
    batch_id: usize,
    output_path: String,
    s3_client: Client,
    bucket_name: String,
    feature_generators: Arc<Vec<Box<dyn FeatureGenerator + Send + Sync>>>
) {
    let prices: Vec<f64> = raw_trades.iter().map(|x| x.price.parse().unwrap()).collect();
    let volumes: Vec<f64> = raw_trades.iter().map(|x| x.volume.parse().unwrap()).collect();
    let times: Vec<i64> = raw_trades.iter().map(|x| x.time).collect();

    let df = DataFrame::new(times.len(), vec![
        Column::new("price".into(), prices),
        Column::new("qty".into(), volumes),
        Column::new("time".into(), times),
    ]).unwrap();

    let mut df = tokio::task::spawn_blocking(move || {
        let mut batch = DataBatch { df };

        for generator in feature_generators.iter() {
            let new_columns = generator.generate(&batch).unwrap();
            batch.df = batch.df.hstack(&new_columns).unwrap();
        }

        // if i > 0 {
        //     batch.df = batch
        //         .df
        //         .slice(tail_size as i64, batch.df.height() - tail_size);
        // }

        batch.df
    }).await.unwrap();

    let target_dir = format!("{}dataset_{}", output_path, chrono::Local::now().format("%Y%m%d"));
    let part_file_name = format!("{}/part_{:04}.parquet", target_dir, batch_id);
    std::fs::create_dir_all(&target_dir).unwrap();
    let mut file = File::create(&part_file_name).unwrap();
    ParquetWriter::new(&mut file).finish(&mut df).unwrap();

    let s3_dataset_dir = format!("dataset_{}", chrono::Local::now().format("%Y%m%d"));
    let s3_key = format!("{}/part_{:04}.parquet", s3_dataset_dir, batch_id);
    upload_to_s3(&s3_client, bucket_name.as_str(), &part_file_name, &s3_key)
                .await
                .unwrap();
}