use crate::batch::DataBatch;
use polars::prelude::PolarsResult;

pub trait FeatureGenerator {
    fn generate(&self, batch: &mut DataBatch) -> PolarsResult<()>;
}