use criterion::{black_box, criterion_group, criterion_main, Criterion};
use polars::prelude::*;

use fast_feature_engine::batch::DataBatch;
use fast_feature_engine::features::{FeatureGenerator, RollingVolatility};

fn bench_rolling_volatility(c: &mut Criterion) {
    let size = 100_000;
    let times: Vec<i64> = (0..size).map(|i| i as i64 * 1000).collect();
    let returns: Vec<f64> = (0..size).map(|_| 0.0001).collect();

    let df = DataFrame::new(size, vec![
        Column::new("time".into(), times),
        Column::new("log_return".into(), returns),
    ]).unwrap();

    let batch = DataBatch { df };

    let generator = RollingVolatility {
        intervals_ms: vec![10_000, 60_000, 600_000],
    };

    c.bench_function("rolling_volatility_100k", |b| {
        b.iter(|| {
            let result = generator.generate(black_box(&batch)).unwrap();
            black_box(result);
        })
    });
}

criterion_group!(benches, bench_rolling_volatility);
criterion_main!(benches);