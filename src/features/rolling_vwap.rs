use crate::batch::DataBatch;
use crate::features::traits::FeatureGenerator;
use polars::prelude::*;

pub struct RollingVwap {
    interval_ms: i64,
}

impl RollingVwap {
    fn compute_rolling_vwap(&self, prices: &[f64], qty: &[f64], times: &[i64]) -> Vec<Option<f64>> {
        let n = times.len();
        let mut result = vec![None; n];

        let mut left = 0usize;
        let mut qty_sum = 0.0;
        let mut transactions_sum = 0.0;

        for right in 0..n {
            let current_qty = qty[right];
            qty_sum += current_qty;
            transactions_sum += current_qty * prices[right];

            while times[right] - times[left] > self.interval_ms {
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
    fn generate(&self, batch: &mut DataBatch) -> PolarsResult<()> {
        let prices = batch.df.column("price").expect("No column price").f64()?;
        let qty = batch.df.column("qty").expect("No column qty").f64()?;
        let times = batch.df.column("times").expect("No column times").i64()?;

        let prices_vec: Vec<f64> = prices.iter().filter_map(|x| x).collect();
        let qty_vec: Vec<f64> = qty.iter().filter_map(|x| x).collect();
        let times_vec: Vec<i64> = times.iter().filter_map(|x| x).collect();

        let rolling_vwap = self.compute_rolling_vwap(&prices_vec, &qty_vec, &times_vec);

        let col_str = match self.interval_ms {
            i if i < 60_000 => {
                format!("rolling_vwap_{}s", self.interval_ms / 1000)
            }
            i if i >= 60_000 => {
                format!("rolling_volatility_{}m", self.interval_ms / (1000 * 60))
            }
            _ => {
                format!("rolling_volatility_{}s", self.interval_ms / 1000)
            }
        };

        let rolling_vwap_col = Column::new(col_str.into(), rolling_vwap);
        batch.df.with_column(rolling_vwap_col)?;
        Ok(())
    }
}
