import os
import yaml
from datetime import datetime
from typing import List, Dict, Any

from colorama import Fore, Style, init

from app.domain.pipeline.trading_pipeline import TradingPipeline
from app.core.settings import settings

init(autoreset=True)


def _load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        base_dir = os.path.dirname(__file__)
        alt_path = os.path.abspath(os.path.join(base_dir, "../../../../", path))
        if os.path.exists(alt_path):
            path = alt_path
        else:
            raise FileNotFoundError(f"Config file not found: {path} or {alt_path}")

    with open(path, "r") as f:
        return yaml.safe_load(f)


# =========================
# UNIVERSE LOADING
# =========================
def _load_universe_from_file(path: str, group: str) -> List[str]:
    # Try direct path
    if not os.path.exists(path):
        # Resolve from project root (backend -> ../../..)
        base_dir = os.path.dirname(__file__)
        alt_path = os.path.abspath(os.path.join(base_dir, "../../../../", path))

        if os.path.exists(alt_path):
            path = alt_path
        else:
            raise FileNotFoundError(f"Universe file not found: {path} or {alt_path}")

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    if group not in data:
        raise ValueError(f"Universe group '{group}' not found in file")

    return data[group]


def _get_universe(config: Dict[str, Any]) -> List[str]:
    uc = config.get("universe_config", {})

    path = uc.get("path")
    group = uc.get("active_group")

    if not path or not group:
        raise ValueError("universe_config.path or active_group missing in config")

    return _load_universe_from_file(path, group)


# =========================
# PRINT HELPERS
# =========================


def _print_header():
    print("\n" + "=" * 80)
    print(Fore.CYAN + Style.BRIGHT + "🚀 TOP TRADING OPPORTUNITIES")
    print("=" * 80)


def _print_table(results: List[Dict[str, Any]]):
    print(
        f"{'SYMBOL':<18} {'SIGNAL':<10} {'TREND':<10} {'ENTRY':<10} {'SL':<10} {'TARGET':<10} {'RR':<6}"
    )
    print("-" * 80)

    for r in results:
        signal = r.get("signal", "NA")

        color = Fore.GREEN if signal == "BUY" else Fore.YELLOW
        if signal == "NO_TRADE":
            color = Fore.RED

        print(
            f"{Fore.CYAN}{r.get('symbol', 'NA'):<18} "
            f"{color}{signal:<10} "
            f"{Fore.WHITE}{r.get('trend', 'NA'):<10} "
            f"{Fore.YELLOW}{str(r.get('entry')):<10} "
            f"{Fore.RED}{str(r.get('stop_loss')):<10} "
            f"{Fore.GREEN}{str(r.get('target')):<10} "
            f"{Fore.CYAN}{str(r.get('rr')):<6}"
        )


def _rank_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def score(r):
        if r.get("signal") != "BUY":
            return 0
        return r.get("rr") or 0

    return sorted(results, key=score, reverse=True)


# =========================
# MAIN SCANNER
# =========================


def run_scanner(config: Dict[str, Any]):
    symbols = _get_universe(config)

    print(Fore.BLUE + f"\n🔍 Scanning {len(symbols)} stocks from universe...\n")

    pipeline = TradingPipeline(config)

    start = datetime(2024, 1, 1)
    end = datetime.now()

    results = []

    import time

    for symbol in symbols:
        try:
            result = pipeline.run(symbol, "D", start, end)
            if result:
                result["symbol"] = symbol
                results.append(result)

            time.sleep(0.2)  # prevent Fyers rate limit (429)

        except Exception as e:
            print(Fore.RED + f"❌ Error processing {symbol}: {e}")
            time.sleep(0.2)

    ranked = _rank_results(results)

    opportunities = [r for r in ranked if r.get("signal") == "BUY"][:10]
    watchlist = [r for r in ranked if r.get("signal") != "BUY"][:10]

    _print_header()

    print(Fore.GREEN + Style.BRIGHT + "\n🔥 TOP OPPORTUNITIES\n")
    _print_table(opportunities)

    print(Fore.YELLOW + Style.BRIGHT + "\n👀 WATCHLIST\n")
    _print_table(watchlist)

    print("\n" + "=" * 80)


# =========================
# ENTRYPOINT
# =========================

if __name__ == "__main__":
    import redis
    from fyers_apiv3 import fyersModel

    # =========================
    # LOAD CONFIGS (NEW STRUCTURE)
    # =========================
    strategy_config = _load_yaml("config/strategy.yaml")
    infra_config = _load_yaml("config/infra_config.yaml")
    scanner_config = _load_yaml("config/scanner_config.yaml")

    # Merge configs (priority: scanner > infra > strategy)
    config = {
        **strategy_config,
        **infra_config,
        **scanner_config,
    }

    # =========================
    # REDIS
    # =========================
    r = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)
    raw = r.get("fyers_access_token")

    if raw is None:
        raise Exception("❌ No token found. Run 'make token'")

    token = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)

    # =========================
    # FYERS CLIENT
    # =========================
    if not settings.FYERS_CLIENT_ID:
        raise Exception("❌ FYERS_CLIENT_ID missing in .env")

    fyers = fyersModel.FyersModel(
        client_id=settings.FYERS_CLIENT_ID,
        token=token,
        is_async=False,
        log_path="",
    )

    # =========================
    # DB (temporary in-memory)
    # =========================
    class DB:
        def __init__(self):
            self.data = {}

        def save_candles(self, symbol, timeframe, df):
            self.data[f"{symbol}:{timeframe}"] = df

        def get_candles(self, symbol, timeframe):
            return self.data.get(f"{symbol}:{timeframe}")

    db = DB()

    # Inject infra into config
    config["redis_client"] = r
    config["db_client"] = db
    config["fyers_client"] = fyers

    # =========================
    # RUN SCANNER
    # =========================
    run_scanner(config)
