import polars as pl
import duckdb
import dtex
import os

script_dir = os.path.dirname(__file__)
nfl = os.path.join(script_dir, "../data/nfl.csv")
postcode = os.path.join(script_dir, "../data/postcode.arrow")

polars_lazy = (
    pl.scan_ipc(postcode).groupby("code_postal").count().sort("count", descending=True)
)
polars_eager = polars_lazy.collect()
arrow = polars_eager.to_arrow()
batch = arrow.to_batches()[0]
duck = duckdb.sql(
    'SELECT code_postal, count(*) as count FROM "data/postcode.csv" GROUP BY code_postal ORDER BY count DESC'
)

dtex.ex(
    [
        ("polars_lazy", polars_lazy),
        ("polars_eager", polars_eager),
        ("pyarrow_table", arrow),
        ("pyarrow_batch", batch),
        ("duckdb", duck),
    ]
)
