use polars::prelude::*;
use std::fmt;
use std::time::Instant;

struct DataBatch {
    df: DataFrame,
}

impl fmt::Display for DataBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.df)
    }
}

trait FeatureGenerator {
    fn generate(&self, batch: &mut DataBatch) -> PolarsResult<()>;
}

struct LogReturnGenerator {
    interval_ms: Option<u64>,
}

impl LogReturnGenerator {
    fn compute_log_return(&self, times: &[i64], prices: &[f64]) -> Vec<Option<f64>> {
        let n = times.len();
        let mut result = vec![None; n];
        let mut j = 0usize;

        for i in 0..n {
            let target_time = times[i] - self.interval_ms.unwrap_or(0) as i64;

            while j + 1 < n && times[j + 1] <= target_time {
                j += 1;
            }

            if times[j] <= target_time && prices[j] > 0.0 {
                result[i] = Some((prices[i] / prices[j]).ln());
            }
        }

        result
    }
}

impl FeatureGenerator for LogReturnGenerator {
    fn generate(&self, batch: &mut DataBatch) -> PolarsResult<()> {
        match self.interval_ms {
            None => {
                let price = batch.df.column("price")?.f64()?;
                let price_prev = price.shift(1);

                let mut log_return: Vec<Option<f64>> = price
                    .into_iter()
                    .zip(price_prev.into_iter())
                    .map(|(curr, prev)| match (curr, prev) {
                        (Some(p), Some(prev_p)) if prev_p > 0.0 => Some((p / prev_p).ln()),
                        _ => None,
                    })
                    .collect();

                if !log_return.is_empty() {
                    log_return[0] = Some(0.0);
                }

                let log_return_col = Column::new(PlSmallStr::from_str("log_return"), log_return);
                batch.df.with_column(log_return_col)?;
            }
            Some(interval) => {
                let col_str = format!("log_return_{}s", interval / 1000);

                let left_lazy = batch
                    .df
                    .clone()
                    .lazy()
                    .with_columns([(col("time") - lit(interval)).alias("target_time")]);

                let right_lazy = batch
                    .df
                    .clone()
                    .lazy()
                    .select([col("time"), col("price").alias("past_price")]);

                let asof_options = AsOfOptions {
                    strategy: AsofStrategy::Backward,
                    tolerance: None,
                    tolerance_str: None,
                    left_by: None,
                    right_by: None,
                    allow_eq: true,
                    check_sortedness: true,
                };

                let final_df = left_lazy
                    .join(
                        right_lazy,
                        [col("target_time")],
                        [col("time")],
                        JoinArgs::new(JoinType::AsOf(Box::new(asof_options))),
                    )
                    .collect()?;

                println!("{}", final_df);

                let price_ca = final_df.column("price")?.f64()?;
                let past_price_ca = final_df.column("past_price")?.f64()?;

                let log_returns: Vec<Option<f64>> = price_ca //Skad mam wiedziec jaki type tutaj dac
                    .into_iter()
                    .zip(past_price_ca.into_iter())
                    .map(|(opt_p, opt_past)| match (opt_p, opt_past) {
                        (Some(p), Some(past)) => Some((p / past).ln()),
                        _ => Some(0.0),
                    })
                    .collect();

                let log_returns_col = Column::new(PlSmallStr::from_str(col_str.as_str()), log_returns);
                batch.df.with_column(log_returns_col)?;
            }
        }
        Ok(())
    }
}

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
