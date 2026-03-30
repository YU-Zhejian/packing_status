import glob
import json
import polars as pl

if __name__ == "__main__":
    print(f"Polars version: {pl.__version__}")
    all_files = glob.glob("monthly/*/*.parquet")
    for fn in all_files:
        print(f"Processing {fn}...", end="")
        try:
            df = pl.read_parquet(fn)
        except Exception as e:
            print("Failed")
            continue
        schema = {k: str(v) for k, v in zip(df.columns, df.dtypes)}
        with open(fn.replace(".parquet", ".schema.json"), "w") as f:
            json.dump(schema, f, indent=4)
            print(json.dumps(schema))
        print(df.head(1))
