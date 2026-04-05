import pandas as pd


def calculate_ema(df: pd.DataFrame, period: int, column: str = "close") -> pd.Series:
    return df[column].ewm(span=period, adjust=False).mean()

