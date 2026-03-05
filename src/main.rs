pub mod batch;
pub mod features;
pub mod reader;
pub mod s3_uploader;

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use batch::DataBatch;
use features::{FeatureGenerator, LogReturnGenerator, RollingVolatility, RollingVwap};
use polars::prelude::*;
use reader::BatchReader;
use s3_uploader::upload_to_s3;
use std::fs::File;
use std::time::Instant;
use tokio::runtime::Runtime;

fn main() -> polars::prelude::PolarsResult<()> {
    dotenvy::dotenv().ok();

    let batch_size = 100_000;
    let tail_size = 5_00;
    let directory_path = "data/";
    let output_path = "output/";

    let target_dir = format!(
        "{}dataset_{}",
        output_path,
        chrono::Local::now().format("%Y%m%d_%H%M%S")
    );
    std::fs::create_dir_all(&target_dir).unwrap();

    let bucket_name = std::env::var("S3_BUCKET_NAME").unwrap();
    let rt = Runtime::new().unwrap();
    let aws_config =
        rt.block_on(async { aws_config::load_defaults(BehaviorVersion::latest()).await });
    let s3_client = Client::new(&aws_config);

    let s3_dataset_dir = format!("dataset_{}", chrono::Local::now().format("%Y%m%d_%H%M%S"));

    let reader = BatchReader::new(directory_path, batch_size, tail_size);

    let feature_generators: Vec<Box<dyn FeatureGenerator + Send + Sync>> = vec![
        Box::new(LogReturnGenerator {
            intervals_ms: vec![None, Some(10_000), Some(60_000), Some(600_000)],
        }),
        Box::new(RollingVolatility {
            intervals_ms: vec![10_000, 60_000, 600_000],
        }),
        Box::new(RollingVwap {
            intervals_ms: vec![10_000, 60_000, 600_000],
        }),
    ];

    let start = Instant::now();
    for (i, current_batch) in reader.enumerate() {
        let mut batch = DataBatch { df: current_batch };
        let now = Instant::now();

        for generator in &feature_generators {
            let new_columns = generator.generate(&batch)?;
            batch.df = batch.df.hstack(&new_columns)?;
        }

        if i > 0 {
            batch.df = batch
                .df
                .slice(tail_size as i64, batch.df.height() - tail_size);
        }

        println!("Batch {}: {}", i, now.elapsed().as_micros());

        let part_file_name = format!("{}/part_{:04}.parquet", target_dir, i);
        let mut file = File::create(&part_file_name).unwrap();
        ParquetWriter::new(&mut file).finish(&mut batch.df).unwrap();

        let s3_key = format!("{}/part_{:04}.parquet", s3_dataset_dir, i);
        rt.block_on(async {
            upload_to_s3(&s3_client, bucket_name.as_str(), &part_file_name, &s3_key)
                .await
                .unwrap();
        });

        println!("Batch {} sent to s3", i);
    }
    println!("END OF PROGRAM IN {}", start.elapsed().as_millis());

    Ok(())
}
