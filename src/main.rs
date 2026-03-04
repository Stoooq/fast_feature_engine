pub mod batch;
pub mod features;
pub mod reader;

use batch::DataBatch;
use features::{FeatureGenerator, LogReturnGenerator, RollingVolatility};
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
    let mut is_first_batch = true;

    let result_file = format!(
        "{}ff_engine_{}.csv",
        output_path,
        chrono::Local::now().format("%Y%m%d_%H%M%S")
    );
    let mut file = File::create(result_file).unwrap();

    let reader = BatchReader::new(directory_path, batch_size, tail_size);

    let feature_generators: Vec<Box<dyn FeatureGenerator>> = vec![
        Box::new(LogReturnGenerator { interval_ms: None }),
        Box::new(LogReturnGenerator {
            interval_ms: Some(1_000 * 10),
        }),
        Box::new(LogReturnGenerator {
            interval_ms: Some(1_000 * 60),
        }),
        Box::new(LogReturnGenerator {
            interval_ms: Some(1_000 * 60 * 10),
        }),
        Box::new(RollingVolatility {
            interval_ms: 1_000 * 60 * 10,
        }),
    ];

    let start = Instant::now();
    for (i, current_batch) in reader.enumerate() {
        // if i > 1 {
        //     break;
        // }
        let mut batch = DataBatch { df: current_batch };

        let now = Instant::now();

        for generator in &feature_generators {
            generator.generate(&mut batch).unwrap();
        }

        if i > 0 {
            batch.df = batch
                .df
                .slice(tail_size as i64, batch.df.height() - tail_size);
        }

        println!("Batch {}: {}", i, now.elapsed().as_micros());

        // println!("{}", batch);

        CsvWriter::new(&mut file)
            .include_header(is_first_batch)
            .finish(&mut batch.df)
            .unwrap();

        is_first_batch = false;
    }
    println!("END OF PROGRAM IN {}", start.elapsed().as_millis());

    Ok(())
}
