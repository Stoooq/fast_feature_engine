use crate::batch::DataBatch;
use crate::features::traits::FeatureGenerator;
use polars::prelude::*;

pub struct LogReturnGenerator {
    pub interval_ms: Option<i64>,
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
                let price = batch.df.column("price").expect("No column price").f64()?;
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
                let prices = batch.df.column("price").expect("No column price").f64()?;
                let times = batch.df.column("time").expect("No column time").i64()?;

                let prices_vec: Vec<f64> = prices.iter().filter_map(|x| x).collect();
                let times_vec: Vec<i64> = times.iter().filter_map(|x| x).collect();

                let values = self.compute_log_return(&times_vec, &prices_vec);

                let col_str = match interval {
                    i if i < 60_000 => {
                        format!("log_return_{}s", interval / 1000)
                    }
                    i if i >= 60_000 => {
                        format!("log_return_{}m", interval / (1000 * 60))
                    }
                    _ => {
                        format!("log_return_{}s", interval / 1000)
                    }
                };

                let log_return_col = Column::new(col_str.into(), values);

                batch.df.with_column(log_return_col)?;
            }
        }
        Ok(())
    }
}
