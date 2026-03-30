import datetime
import numpy as np
import polars as pl

if __name__ == "__main__":
    print(f"Polars version: {pl.__version__}")
    df_all = pl.read_parquet("packing_status_all.parquet")

    # Median counts per (data_source, pkg_name)
    df_median = df_all.group_by(["data_source", "pkg_name"]).agg(pl.col(["counts"]).median().alias("counts"))
    df_median.write_parquet("packing_status_median.parquet")
    print(f"Wrote packing_status_median.parquet with {df_median.height} rows.")

    # Total counts per (data_source, pkg_name)
    df_total = df_all.group_by(["data_source", "pkg_name"]).agg(pl.col(["counts"]).sum().alias("counts"))
    df_total.write_parquet("packing_status_total.parquet")
    print(f"Wrote packing_status_total.parquet with {df_total.height} rows.")

    # Conda-Velocity score
    # Convert all months to year * 12 + month integer
    df_all = df_all.with_columns([
        (pl.col("time").str.split("-").list.get(0).cast(pl.Int32) * 12 + pl.col("time").str.split("-").list.get(1).cast(pl.Int32)).alias("time_int")
    ])
    now = datetime.datetime.now()
    current_time_int = now.year * 12 + now.month
    df_all = df_all.with_columns([
        (current_time_int - pl.col("time_int")).alias("age_months")
    ])
    df_recent = df_all.filter(pl.col("age_months") <= 12)
    k = 0.15
    df_recent = df_recent.with_columns([
        (np.exp(pl.col("age_months") * -k)).alias("decay_factor"),
        (np.exp(pl.col("counts") * pl.col("age_months") * -k)).alias("weighted_counts")
    ])
    # Correction: weighted_counts should be counts * decay_factor
    df_recent = df_recent.with_columns([
        (pl.col("counts") * pl.col("decay_factor")).alias("weighted_counts")
    ])
    df_velocity = df_recent.group_by(["data_source", "pkg_name"]).agg(pl.col(["weighted_counts"]).sum().alias("counts"))
    df_velocity.write_parquet("packing_status_conda_velocity.parquet")
    print(f"Wrote packing_status_conda_velocity.parquet with {df_velocity.height} rows.")
