# data

`sample-prices.csv` is a **synthetic** price series (a seeded random walk, one row per second, starting at 67,000 and generated once with Python's `random.seed(20261110)`, Gaussian steps with sigma 0.04%), not a recording of a real market. It exists so the bot and the tests run offline and deterministically. To use real prices, record a ticker feed into a CSV with a `price` column (or a JSON array of numbers or of `{"price": ...}`) and pass it with `bot -prices`.
