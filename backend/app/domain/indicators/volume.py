import pandas as pd


def calculate_volume_ma(df: pd.DataFrame, period: int) -> pd.Series:
    return df["volume"].rolling(window=period).mean()
