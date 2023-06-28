py:
    pip install ./py-dtex

example:
    cd example && pip3 install -r requirements.txt
    python3 example/full.py
    python3 example/demo.py

run-file:
    cargo run -- data/postcode.csv data/postcode.parquet data/postcode.ndjson data/postcode.json

run-sql:
    cargo run -- data/single.sql data/multi.sql data/empty.sql

run-all: run-file run-sql