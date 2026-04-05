import pandas as pd


def calculate_sma(df: pd.DataFrame, period: int, column: str = "close") -> pd.Series:
    return df[column].rolling(window=period).mean()
