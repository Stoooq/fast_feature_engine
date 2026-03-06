# Rust Quant Feature Engine

## Overview
A high-performance, stateful Feature Engineering engine built in Rust. Designed to process massive financial time-series data and generate multi-horizon features (e.g., Rolling Volatility, VWAP) for Machine Learning models.  
The system operates in two distinct modes:
- Offline mode: Processes historical CSV data files in blocks, using minimal memory, and then transforms them into Parquet files ready for machine learning.  
- Live Mode: Connects to the Binance WebSocket, buffers live trades, and computes rolling features in real-time.

All processed batches are automatically uploaded to AWS S3 Data Lake for possible model training using Python.

## Performance Benchmark: Rust vs Python

Comparative analysis of Rolling Volatility feature generation (computing 3 parallel time horizons: 10 s, 1 min, 10 min) on a synthetic dataset of **100,000 ticks**. Time was measured using `criterion` (Rust) and `timeit` (Python).

| Language / Library | Execution Time (Avg) | Concurrency Model | Speedup vs Python |
| :--- | :--- | :--- | :--- |
| **Python (Pandas)** | `9.01 ms` | Single-threaded (GIL blocked) | 1.0x (Baseline) |
| **Rust (Polars)** | `3.60 ms` | Single-threaded | **2.5x faster** |
| **Rust (Polars + Rayon)**| `2.42 ms` | Multi-threaded | **3.7x faster** |

## Engineering Highlights
- Stateful Chunking for Rolling Windows: Solved the boundary problem in batch processing. The engine seamlessly passes a "tail" buffer between consecutive chunks to ensure that time-based features (like 10-minute rolling volatility) have the correct historical context, without data duplication.
- CPU vs I/O Optimization: Implemented a hybrid concurrency model. Uses rayon for parallel CPU-bound mathematical computations (Feature Generation) across all CPU cores, while utilizing tokio (async) for network I/O operations (WebSocket streams and S3 uploads) to prevent blocking.
- Zero-Copy & Memory Efficiency: Leverages Polars to execute zero-copy data transformations. Processing 100k+ rows in under 100ms for one feature.
- Data Lake Integration: Bypasses CSV limitations by natively exporting highly compressed .parquet files, dramatically accelerating downstream ML training workflows.

## System Architecture
1. Offline Pipeline  
Large CSV Files -> Stateful BatchReader -> Rayon Parallel Feature Gen -> Parquet Writer -> AWS S3

2. Live Pipeline  
Binance WebSocket (@trade) -> Tokio Async Buffer + Tail Mgt -> Rayon Parallel Feature Gen -> Parquet Writer -> AWS S3

## Generated Features
For demonstration purposes, the engine computes the following core ML features over various horizons (e.g., 10s, 1m, 10m). The system is fully extensible and easily scales to support any number of custom indicators:
- Multi-horizon Log Returns
- Rolling Volatility
- VWAP (Volume Weighted Average Price)

## Tech Stack
Language: Rust 🦀  
Data Processing: Polars, Apache Arrow  
Concurrency / Async: Rayon, Tokio  
Cloud / Storage: AWS SDK (S3), Parquet  
Networking: Tokio-tungstenite  
Benchmarking: Criterion  
Configuration: Serde, TOML  

## Quick Start
1. Clone the repository.
2. Configure AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
3. Adjust parameters in config.toml (select mode = "offline" or mode = "live").
4. Run the engine: cargo run.
