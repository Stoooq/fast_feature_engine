pub mod batch;
pub mod features;

use batch::DataBatch;
use features::{FeatureGenerator, LogReturnGenerator};
use polars::prelude::*;
use std::time::Instant;

fn main() -> polars::prelude::PolarsResult<()> {
    let df = CsvReadOptions::default()
        .with_n_rows(Some(40000))
        .try_into_reader_with_file_path(Some("data/BTCUSDT-trades-2025-01.csv".into()))
        .unwrap()
        .finish()
        .unwrap();

    let mut batch = DataBatch { df };

    let generator = LogReturnGenerator {
        interval_ms: Some(5_000),
    };

    let now = Instant::now();
    generator.generate(&mut batch)?;
    println!("{}", now.elapsed().as_micros());

    println!("{}", batch);

    Ok(())
}
