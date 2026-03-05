use crate::batch::DataBatch;
use crate::features::traits::FeatureGenerator;
use polars::prelude::*;
use rayon::prelude::*;

pub struct RollingVolatility {
    pub intervals_ms: Vec<u64>,
}

impl RollingVolatility {
    fn compute_rolling_volatility(
        &self,
        times: &[i64],
        log_returns: &[f64],
        interval: u64,
    ) -> Vec<Option<f64>> {
        let n = times.len();
        let mut result = vec![None; n];

        let mut left = 0usize;
        let mut sum = 0.0;
        let mut sum_sq = 0.0;

        for right in 0..n {
            let current_return = log_returns[right];
            sum += current_return;
            sum_sq += current_return * current_return;

            while times[right] - times[left] > interval as i64 {
                let old_return = log_returns[left];
                sum -= old_return;
                sum_sq -= old_return * old_return;
                left += 1;
            }

            let count_in_window = (right - left + 1) as f64;

            if count_in_window > 1.0 {
                let mut variance = (sum_sq - (sum * sum / count_in_window)) / count_in_window;

                if variance < 0.0 {
                    variance = 0.0;
                }

                result[right] = Some(variance.sqrt());
            } else {
                result[right] = Some(0.0);
            }
        }

        result
    }
}

impl FeatureGenerator for RollingVolatility {
    fn generate(&self, batch: &DataBatch) -> PolarsResult<Vec<Column>> {
        let times = batch.df.column("time").expect("No column time").i64()?;
        let log_returns = batch
            .df
            .column("log_return")
            .expect("No column log_return")
            .f64()?;

        let times_vec: Vec<i64> = times.into_iter().map(|x| x.unwrap_or(0)).collect();
        let log_returns_vec: Vec<f64> = log_returns.into_iter().map(|x| x.unwrap_or(0.0)).collect();

        let columns: Result<Vec<Column>, _> = self
            .intervals_ms
            .par_iter()
            .map(|&interval| {
                let rolling_volatility =
                    self.compute_rolling_volatility(&times_vec, &log_returns_vec, interval);

                let col_str = match interval {
                    i if i < 60_000 => {
                        format!("rolling_volatility_{}s", interval / 1000)
                    }
                    i if i >= 60_000 => {
                        format!("rolling_volatility_{}m", interval / (1000 * 60))
                    }
                    _ => {
                        format!("rolling_volatility_{}s", interval / 1000)
                    }
                };

                Ok(Column::new(col_str.into(), rolling_volatility))
            })
            .collect();

        columns
    }
}
