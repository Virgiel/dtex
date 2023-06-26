py:
    pip install ./py-dtex

example:
    cd example && pip3 install -r requirements.txt
    python3 example/full.py
    python3 example/demo.py