use crate::batch::DataBatch;
use crate::features::traits::FeatureGenerator;
use polars::prelude::*;
use rayon::prelude::*;

pub struct LogReturnGenerator {
    pub intervals_ms: Vec<Option<u64>>,
}

impl LogReturnGenerator {
    fn compute_log_return(&self, times: &[i64], prices: &[f64], interval: u64) -> Vec<Option<f64>> {
        let n = times.len();
        let mut result = vec![None; n];
        let mut j = 0usize;

        for i in 0..n {
            let target_time = times[i] - interval as i64;

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
    fn generate(&self, batch: &DataBatch) -> PolarsResult<Vec<Column>> {
        let prices = batch.df.column("price").expect("No column price").f64()?;
        let times = batch.df.column("time").expect("No column time").i64()?;
        let prices_prev = prices.shift(1);

        let prices_vec: Vec<f64> = prices.into_iter().map(|x| x.unwrap_or(0.0)).collect();
        let times_vec: Vec<i64> = times.into_iter().map(|x| x.unwrap_or(0)).collect();

        let columns: Result<Vec<Column>, _> = self
            .intervals_ms
            .par_iter()
            .map(|&interval| match interval {
                None => {
                    let mut log_return: Vec<Option<f64>> = prices
                        .into_iter()
                        .zip(prices_prev.into_iter())
                        .map(|(curr, prev)| match (curr, prev) {
                            (Some(p), Some(prev_p)) if prev_p > 0.0 => Some((p / prev_p).ln()),
                            _ => None,
                        })
                        .collect();

                    if !log_return.is_empty() {
                        log_return[0] = Some(0.0);
                    }

                    Ok(Column::new("log_return".into(), log_return))
                }
                Some(interval) => {
                    let values = self.compute_log_return(&times_vec, &prices_vec, interval);

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

                    Ok(Column::new(col_str.into(), values))
                }
            })
            .collect();

        columns
    }
}
