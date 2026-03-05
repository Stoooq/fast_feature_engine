use crate::batch::DataBatch;
use polars::prelude::{Column, PolarsResult};

pub trait FeatureGenerator: Send + Sync {
    fn generate(&self, batch: &DataBatch) -> PolarsResult<Vec<Column>>;
}
