import polars as pl
import dtex
import os
script_dir = os.path.dirname(__file__)
data_path = "../../data/nfl.csv"
path = os.path.join(script_dir, data_path)


df = pl.read_csv(path).groupby('gameid').count().sort('count', descending=True)
print(df)
print(dtex.ex(df))