pub mod batch;
pub mod features;
pub mod reader;

use batch::DataBatch;
use features::{FeatureGenerator, LogReturnGenerator, RollingVolatility, RollingVwap};
use polars::prelude::*;
use reader::BatchReader;
use std::fs::File;
use std::time::Instant;

fn main() -> polars::prelude::PolarsResult<()> {
    // unsafe {
    //     std::env::set_var("POLARS_FMT_MAX_ROWS", "50");
    // }

    let batch_size = 100_000;
    let tail_size = 5_00;
    let directory_path = "data/";
    let output_path = "output/";

    let target_dir = format!("{}dataset_{}", output_path, chrono::Local::now().format("%Y%m%d_%H%M%S"));
    std::fs::create_dir_all(&target_dir).expect("Nie udało się utworzyć folderu docelowego");

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
    }
    println!("END OF PROGRAM IN {}", start.elapsed().as_millis());

    Ok(())
}
