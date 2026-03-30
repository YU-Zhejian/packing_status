import glob
import polars as pl

if __name__ == "__main__":
    print(f"Polars version: {pl.__version__}")

    all_files = glob.glob("monthly/*/*.parquet")
    odfs = []
    for fn in all_files:
        print(f"Processing {fn}...", end="")
        try:
            df = pl.read_parquet(fn)
        except Exception as e:
            print("Failed")
            continue
        # Ensure data_source is string
        df = df.with_columns([
            pl.col("data_source").cast(pl.Utf8),
            pl.col("pkg_name").cast(pl.Utf8),
        ])
        # Filter for conda-forge or bioconda
        df_sel = df.filter(
            (pl.col("data_source") == "conda-forge") | (pl.col("data_source") == "bioconda")
        )
        # Group by and sum counts
        df_sel = df_sel.group_by(["time", "data_source", "pkg_name"]).agg(
            pl.col("counts").sum().alias("counts")
        )
        odfs.append(df_sel)
        print("Done")
    if odfs:
        df_all = pl.concat(odfs)
        df_all.write_parquet("packing_status_all.parquet")
        print(f"Wrote packing_status_all.parquet with {df_all.height} rows.")
