use polars::prelude::*;
use std::fmt;

pub struct DataBatch {
    pub df: DataFrame,
}

impl fmt::Display for DataBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.df)
    }
}
