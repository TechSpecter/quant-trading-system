from pathlib import Path
import yaml
from app.core.settings import settings


def load_universe():
    """Load symbols from universe.yaml based on active group in trading_config.yaml."""
    BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

    # Load trading config
    trading_config_path = BASE_DIR / "config/trading_config.yaml"

    if not trading_config_path.exists():
        raise FileNotFoundError(f"Trading config not found at {trading_config_path}")

    with open(trading_config_path, "r") as f:
        trading_config = yaml.safe_load(f) or {}

    universe_config = trading_config.get("universe_config", {})

    universe_rel_path = universe_config.get("path", "config/universe.yaml")
    active_group = universe_config.get("active_group", "nifty50")

    universe_path = BASE_DIR / universe_rel_path

    if not universe_path.exists():
        raise FileNotFoundError(f"Universe file not found at {universe_path}")

    with open(universe_path, "r") as f:
        data = yaml.safe_load(f) or {}

    if active_group not in data:
        raise ValueError(f"Active group '{active_group}' not found in {universe_path}")

    return data[active_group]
