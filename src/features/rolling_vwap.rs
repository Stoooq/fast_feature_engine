use crate::batch::DataBatch;
use crate::features::traits::FeatureGenerator;
use polars::prelude::*;
use rayon::prelude::*;

pub struct RollingVwap {
    pub intervals_ms: Vec<u64>,
}

impl RollingVwap {
    fn compute_rolling_vwap(
        &self,
        prices: &[f64],
        qty: &[f64],
        times: &[i64],
        interval: u64,
    ) -> Vec<Option<f64>> {
        let n = times.len();
        let mut result = vec![None; n];

        let mut left = 0usize;
        let mut qty_sum = 0.0;
        let mut transactions_sum = 0.0;

        for right in 0..n {
            let current_qty = qty[right];
            qty_sum += current_qty;
            transactions_sum += current_qty * prices[right];

            while times[right] - times[left] > interval as i64 {
                let old_qty = qty[left];
                qty_sum -= old_qty;
                transactions_sum -= old_qty * prices[left];
                left += 1
            }

            let count_in_window = (right - left + 1) as f64;

            if count_in_window > 1.0 {
                let mut vwap = transactions_sum / qty_sum;

                if vwap < 0.0 {
                    vwap = 0.0;
                }

                result[right] = Some(vwap.ln());
            } else {
                result[right] = Some(0.0);
            }
        }

        result
    }
}

impl FeatureGenerator for RollingVwap {
    fn generate(&self, batch: &DataBatch) -> PolarsResult<Vec<Column>> {
        let prices = batch.df.column("price").expect("No column price").f64()?;
        let qty = batch.df.column("qty").expect("No column qty").f64()?;
        let times = batch.df.column("time").expect("No column time").i64()?;

        let prices_vec: Vec<f64> = prices.into_iter().map(|x| x.unwrap_or(0.0)).collect();
        let qty_vec: Vec<f64> = qty.into_iter().map(|x| x.unwrap_or(0.0)).collect();
        let times_vec: Vec<i64> = times.into_iter().map(|x| x.unwrap_or(0)).collect();

        let columns: Result<Vec<Column>, _> = self
            .intervals_ms
            .par_iter()
            .map(|&interval| {
                let rolling_vwap =
                    self.compute_rolling_vwap(&prices_vec, &qty_vec, &times_vec, interval);

                let col_str = match interval {
                    i if i < 60_000 => {
                        format!("rolling_vwap_{}s", interval / 1000)
                    }
                    i if i >= 60_000 => {
                        format!("rolling_vwap_{}m", interval / (1000 * 60))
                    }
                    _ => {
                        format!("rolling_vwap_{}s", interval / 1000)
                    }
                };

                Ok(Column::new(col_str.into(), rolling_vwap))
            })
            .collect();

        columns
    }
}
