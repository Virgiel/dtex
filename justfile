py:
    pip install ./py-dtex

example-py:
    cd example/py && pip3 install -r requirements.txt
    python3 example/py/run.py