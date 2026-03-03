pub mod batch;
pub mod features;
pub mod reader;

use batch::DataBatch;
use features::{FeatureGenerator, LogReturnGenerator};
use reader::BatchReader;
use std::time::Instant;

fn main() -> polars::prelude::PolarsResult<()> {
    let batch_size = 50_000;
    let tail_size = 5_000;
    let directory_path = "data/";

    let reader = BatchReader::new(directory_path, batch_size, tail_size);

    let generator = LogReturnGenerator {
        interval_ms: Some(1_000),
    };

    for (i, current_batch) in reader.enumerate() {
        let mut batch = DataBatch { df: current_batch };

        let now = Instant::now();
        generator.generate(&mut batch)?;
        println!("Batch {}: {}", i, now.elapsed().as_micros());

        println!("{}", batch);
    }

    Ok(())
}
