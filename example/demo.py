import polars as pl
import dtex

polars = (
    pl.scan_csv("data/postcode.csv").groupby("code_postal").count().sort("count", descending=True)
)

dtex.ex([polars])