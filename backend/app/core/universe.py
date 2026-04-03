from pathlib import Path
import yaml
from app.core.settings import settings


def load_universe():
    """Load symbols from universe.yaml based on active group in config."""
    # Project root
    BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent

    # Read from settings if available, else fallback
    universe_rel_path = getattr(settings, "UNIVERSE_PATH", "config/universe.yaml")
    active_group = getattr(settings, "ACTIVE_UNIVERSE_GROUP", "nifty50")

    universe_path = BASE_DIR / universe_rel_path

    if not universe_path.exists():
        raise FileNotFoundError(f"Universe file not found at {universe_path}")

    with open(universe_path, "r") as f:
        data = yaml.safe_load(f) or {}

    symbols = data.get(active_group)

    if not symbols:
        raise ValueError(
            f"Active group '{active_group}' not found or empty in {universe_path}"
        )

    return symbols
