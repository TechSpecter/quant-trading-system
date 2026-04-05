import pandas as pd


def is_bull_trend(row: pd.Series, config) -> bool:
    trend_rules = config.get("trend", {}).get("rules", [])

    if not trend_rules:
        return False

    for rule in trend_rules:
        left_key = rule.get("left")
        right_key = rule.get("right")

        left = row.get(left_key) or row.get(str(left_key).lower())
        right = row.get(right_key) or row.get(str(right_key).lower())

        if left is None or right is None:
            return False

        try:
            left = float(left)
            right = float(right)
        except Exception:
            return False

        op = rule.get("operator")

        if op == ">" and not (left > right):
            return False
        if op == "<" and not (left < right):
            return False
        if op == ">=" and not (left >= right):
            return False
        if op == "<=" and not (left <= right):
            return False

    return True


def is_bear_trend(row: pd.Series, config) -> bool:
    trend_rules = config.get("trend", {}).get("rules", [])

    if not trend_rules:
        return False

    for rule in trend_rules:
        left_key = rule.get("left")
        right_key = rule.get("right")

        left = row.get(left_key) or row.get(str(left_key).lower())
        right = row.get(right_key) or row.get(str(right_key).lower())

        if left is None or right is None:
            return False

        try:
            left = float(left)
            right = float(right)
        except Exception:
            return False

        op = rule.get("operator")

        # Reverse logic for bear trend
        if op == ">" and not (left < right):
            return False
        if op == "<" and not (left > right):
            return False
        if op == ">=" and not (left <= right):
            return False
        if op == "<=" and not (left >= right):
            return False

    return True
