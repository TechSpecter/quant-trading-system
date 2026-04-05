import pandas as pd


class SimpleDB:
    def __init__(self):
        self.storage = {}

    def save_candles(self, symbol, timeframe, df):
        self.storage[f"{symbol}:{timeframe}"] = df

    def get_candles(self, symbol, timeframe):
        return self.storage.get(f"{symbol}:{timeframe}")


def test_db_save_and_fetch():
    db = SimpleDB()

    df = pd.DataFrame({"close": [100, 101, 102]})

    db.save_candles("TEST", "D", df)

    fetched = db.get_candles("TEST", "D")

    assert fetched is not None
    assert not fetched.empty
    assert len(fetched) == 3

    print("✅ DB working")
