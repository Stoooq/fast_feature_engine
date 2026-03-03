use crate::batch::DataBatch;
use crate::features::traits::FeatureGenerator;
use polars::prelude::*;

pub struct LogReturnGenerator {
    pub interval_ms: Option<i64>,
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

                let log_returns_col =
                    Column::new(PlSmallStr::from_str(col_str.as_str()), log_returns);
                batch.df.with_column(log_returns_col)?;
            }
        }
        Ok(())
    }
}
