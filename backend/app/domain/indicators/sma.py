from typing import Union
import pandas as pd


def calculate_sma(
    df: Union[pd.DataFrame, pd.Series],
    period: int,
    column: str = "close",
) -> pd.Series:
    if df is None:
        return pd.Series(dtype=float)

    # Handle Series input
    if isinstance(df, pd.Series):
        series = df
    else:
        # Safe column access
        if column not in df.columns:
            return pd.Series(dtype=float)
        series = df[column]

    return series.rolling(window=period).mean()
