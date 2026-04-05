from typing import Dict, Any, Optional
from typing import cast
import pandas as pd

from app.domain.risk.stop_loss import calculate_stop_loss
from app.domain.risk.target import calculate_target


def _validate_inputs(entry_price: Optional[float]) -> bool:
    return entry_price is not None and entry_price > 0


def _calculate_rr(entry: float, sl: float, target: float) -> Optional[float]:
    try:
        risk = entry - sl
        reward = target - entry

        if risk <= 0:
            return None

        return round(reward / risk, 2)
    except Exception:
        return None


def evaluate_risk(
    df: pd.DataFrame, entry_price: Optional[float], config: Dict[str, Any]
) -> Dict[str, Any]:

    if not _validate_inputs(entry_price):
        return {"stop_loss": None, "target": None, "rr": None}

    entry_price = float(cast(float, entry_price))

    stop_loss = calculate_stop_loss(df, entry_price, config)

    if stop_loss is None:
        return {"stop_loss": None, "target": None, "rr": None}

    stop_loss = float(stop_loss)

    target = calculate_target(df, entry_price, stop_loss, config)

    if target is None:
        return {"stop_loss": stop_loss, "target": None, "rr": None}

    target = float(target)

    rr = _calculate_rr(entry_price, stop_loss, target)

    return {"stop_loss": stop_loss, "target": target, "rr": rr}
