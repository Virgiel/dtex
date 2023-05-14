py:
    pip install ./py-dtex

example-py:
    cd example/py && pip3 install -r requirements.txt
    python3 example/py/run.py

example-node:
    cd node-dtex && pnpm install && pnpm build
    cd example/node && pnpm install && pnpm start

example: example-py example-node