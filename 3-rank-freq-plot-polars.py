import polars as pl
import matplotlib.pyplot as plt

pkgs_to_highlight = [
    "conda-forge::numpy",
    "conda-forge::pandas",
    "conda-forge::polars",
    "conda-forge::pyspark",
    "bioconda::samtools",
    "bioconda::art",
    "bioconda::art_modern",
    "bioconda::art_modern-openmpi",
    "bioconda::bwa",
    "bioconda::gatk4",
]

if __name__ == "__main__":
    for name in ["median", "total", "conda_velocity"]:
        df = pl.read_parquet(f"packing_status_{name}.parquet")
        for channel in ["conda-forge", "bioconda"]:
            pkgs_to_highlight_channel = {
                pkg.replace(f"{channel}::", "") for pkg in pkgs_to_highlight if pkg.startswith(f"{channel}::")
            }
            this_df = df.filter((pl.col("data_source") == channel) & (pl.col("counts") >= 10))
            this_df = this_df.sort("counts", descending=True)
            if this_df.height == 0:
                continue
            top_and_low = this_df.select(["pkg_name", "counts"]).head(1).vstack(this_df.select(["pkg_name", "counts"]).tail(1))
            print(f"\n{name.capitalize()} packing status for channel {channel}:")
            print(top_and_low)
            pkgs_to_highlight_channel.update(top_and_low["pkg_name"].to_list())

            this_df = this_df.with_columns([
                (pl.arange(1, this_df.height + 1)).alias("RANK")
            ])
            plt.figure(figsize=(10, 6))
            plt.loglog(this_df["RANK"], this_df["counts"], linestyle="-", alpha=0.5)
            this_df_highlight = this_df.filter(pl.col("pkg_name").is_in(list(pkgs_to_highlight_channel)))
            plt.loglog(
                this_df_highlight["RANK"],
                this_df_highlight["counts"],
                marker="o",
                color="red",
                linestyle="none",
            )
            for row in this_df_highlight.iter_rows(named=True):
                pkg_name = row["pkg_name"]
                rank = row["RANK"]
                count = row["counts"]
                plt.text(rank, count, pkg_name, fontsize=9, ha="right", va="bottom")
            plt.xlabel("Rank")
            plt.ylabel(f"{name.capitalize()} Packing Status Count")
            plt.title(f"Rank-Frequency Plot of {name.capitalize()} Packing Status Counts")
            plt.grid(True, which="both", ls="--", lw=0.5)
            plt.savefig(f"packing_status_{name}_{channel}_rank_freq_plot.pdf")
            plt.cla()
