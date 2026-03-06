import timeit

import numpy as np
import pandas as pd


def setup_data():
    size = 100_000
    print(f"Preparing data: {size}")

    times = pd.date_range("2026-01-01", periods=size, freq="s")

    returns = np.full(size, 0.0001)

    df = pd.DataFrame({"time": times, "log_return": returns})

    df = df.set_index("time")
    return df


def compute_rolling_volatility(df):
    df["vol_10s"] = df["log_return"].rolling("10s").std()
    df["vol_60s"] = df["log_return"].rolling("60s").std()
    df["vol_10m"] = df["log_return"].rolling("10min").std()
    return df


if __name__ == "__main__":
    df = setup_data()

    print("Benchmark start")

    num_runs = 10

    timer = timeit.Timer(lambda: compute_rolling_volatility(df.copy()))
    run_times = timer.repeat(repeat=num_runs, number=1)

    run_times_ms = [t * 1000 for t in run_times]
    avg_time_ms = sum(run_times_ms) / num_runs

    print("-" * 30)
    print("Example measurments")
    for i in range(min(3, num_runs)):
        print(f"{i + 1}: {run_times_ms[i]:.2f} ms")

    print("-" * 30)
    print(f"Avg time: {avg_time_ms:.2f} ms")
    print("-" * 30)
