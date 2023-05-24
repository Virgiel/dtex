import polars as pl
import dtex
import os
script_dir = os.path.dirname(__file__)
nfl = os.path.join(script_dir, "../data/nfl.csv")
postcode = os.path.join(script_dir, "../data/postcode.arrow")


dtex.ex([
pl.scan_csv(nfl).groupby('gameid').count().sort('count', descending=True).collect(),
pl.scan_ipc(postcode).groupby('code_postal').count().sort('count', descending=True).collect(),
])