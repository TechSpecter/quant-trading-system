import asyncio
import time
import os
import sys
from contextlib import redirect_stdout

from tabulate import tabulate

# -----------------------------
# CONFIG
# -----------------------------
DEBUG = False  # Toggle detailed logs
os.environ["APP_DEBUG"] = "1" if DEBUG else "0"


# ANSI Colors
class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    RESET = "\033[0m"


from app.domain.strategies.strategy_service import StrategyService
from app.core.universe import load_universe
from app.db.session import async_session


async def main():
    print("🚀 Script started")
    import logging

    logging.getLogger().setLevel(logging.ERROR if not DEBUG else logging.INFO)

    # Load universe
    symbols = load_universe()
    total_symbols = len(symbols)
    if DEBUG:
        print("📊 Loaded symbols:", symbols)

    if not symbols:
        print("❌ No symbols found. Check universe.yaml path/config")
        return

    async with async_session() as db:
        if DEBUG:
            print("✅ DB session created")

        service = StrategyService(db)
        universe_name = getattr(service, "universe_name", None)
        if DEBUG:
            print("🧠 MTF Config Loaded from YAML")

        if DEBUG:
            print("⚙️ Running strategy (MTF enabled)...")
        start = time.time()

        # 🔇 Suppress noisy logs from underlying services when DEBUG=False
        if not DEBUG:
            with open(os.devnull, "w") as f, redirect_stdout(f):
                results = await service.process_universe(symbols)
        else:
            results = await service.process_universe(symbols)

        duration = round(time.time() - start, 2)

        if DEBUG:
            print("\n🎯 FINAL RESULTS (Sorted by Score)")
            print(f"⏱️ Execution time: {duration}s\n")

        # Minimal progress logs (only in DEBUG)
        if DEBUG:
            for r in results:
                symbol = r.get("symbol")
                print(f"Processing {symbol}")

        def color_signal(signal):
            if signal in ["BUY", "STRONG_BUY"]:
                return f"{Colors.GREEN}{signal}{Colors.RESET}"
            elif signal in ["WEAK_BUY"]:
                return f"{Colors.YELLOW}{signal}{Colors.RESET}"
            elif signal in ["SELL", "STRONG_SELL"]:
                return f"{Colors.RED}{signal}{Colors.RESET}"
            return signal

        def color_entry(entry):
            if entry == "BUY_NOW":
                return f"{Colors.GREEN}{entry}{Colors.RESET}"
            elif entry == "WAIT":
                return f"{Colors.YELLOW}{entry}{Colors.RESET}"
            elif entry == "WATCH":
                return f"{Colors.CYAN}{entry}{Colors.RESET}"
            elif entry == "AVOID":
                return f"{Colors.RED}{entry}{Colors.RESET}"
            return entry or ""

        # -----------------------------
        # CLEAN & SORT RESULTS
        # -----------------------------
        results = sorted(results, key=lambda x: x.get("score", 0), reverse=True)

        high_conviction = [r for r in results if r.get("score", 0) >= 60]
        watchlist = [r for r in results if 20 <= r.get("score", 0) < 60]
        others = [r for r in results if r.get("score", 0) < 20]

        def build_table(data):
            rows = []
            for r in data:
                gs = r.get("gate_summary", {})

                trend = gs.get("gate_1_trend", {})
                # 🔥 FIX: normalize keys coming from engine
                if trend:
                    trend = {
                        "status": trend.get("status") or "NA",
                        "close": trend.get("close") or trend.get("price"),
                        "ema_50": trend.get("ema_50")
                        or trend.get("EMA_50")
                        or trend.get("ema50"),
                        "ema_20": trend.get("ema_20")
                        or trend.get("EMA_20")
                        or trend.get("ema20"),
                        "sma_200": trend.get("sma_200")
                        or trend.get("SMA_200")
                        or trend.get("sma"),
                        "rule_debug": trend.get("rule_debug"),
                    }
                pullback = gs.get("gate_2_pullback", {})
                trigger = gs.get("gate_3_trigger", {})

                trend_status = trend.get("status") or "NA"
                close = trend.get("close")
                ema50 = trend.get("ema_50")
                sma200 = trend.get("sma_200")
                ema20 = trend.get("ema_20")

                rule_debug = trend.get("rule_debug")

                if (
                    close is not None
                    and sma200 is not None
                    and ema50 is not None
                    and ema20 is not None
                ):
                    cond1_ok = close > sma200
                    cond2_ok = ema50 > sma200
                    cond3_ok = ema20 > ema50

                    cond1 = f"Price>SMA200 {'✅' if cond1_ok else '❌'}"
                    cond2 = f"EMA50>SMA200 {'✅' if cond2_ok else '❌'}"
                    cond3 = f"EMA20>EMA50 {'✅' if cond3_ok else '❌'}"

                    trend_val = f"{trend_status} | {cond1} | {cond2} | {cond3}"
                elif rule_debug:
                    trend_val = f"{trend_status} | {rule_debug}"
                else:
                    trend_val = trend_status

                if pullback:
                    pb_status = pullback.get("status", "")
                    pb_ok = pb_status == "PASS"
                    ema20_pb = pullback.get("ema_20")
                    rsi_pb = pullback.get("rsi")

                    ema20_txt = (
                        f"{round(ema20_pb,2)}"
                        if isinstance(ema20_pb, (int, float))
                        else ""
                    )
                    rsi_txt = (
                        f"{round(rsi_pb,2)}" if isinstance(rsi_pb, (int, float)) else ""
                    )

                    pullback_val = f"{pb_status} {'✅' if pb_ok else '❌'} | EMA20={ema20_txt} | RSI={rsi_txt}"
                else:
                    pullback_val = ""

                if trigger:
                    tg_status = trigger.get("status", "")
                    tg_ok = tg_status == "PASS"
                    ema5 = trigger.get("ema_5")

                    ema5_txt = (
                        f"{round(ema5,2)}" if isinstance(ema5, (int, float)) else ""
                    )
                    trigger_val = (
                        f"{tg_status} {'✅' if tg_ok else '❌'} | EMA5={ema5_txt}"
                    )
                else:
                    trigger_val = ""

                entry = r.get("entry_signal")
                reason = r.get("trigger_reason") or ""
                sl = r.get("stop_loss")
                sl_val = round(sl, 2) if isinstance(sl, (int, float)) else ""

                last_price = round(close, 2) if isinstance(close, (int, float)) else ""

                # ✅ Entry price from engine (fallback to last price if missing)
                entry_raw = r.get("entry_price")
                entry_price = (
                    round(entry_raw, 2)
                    if isinstance(entry_raw, (int, float))
                    else last_price
                )

                target = r.get("target_price")
                target_price = (
                    round(target, 2) if isinstance(target, (int, float)) else ""
                )

                if (
                    isinstance(entry_price, (int, float))
                    and isinstance(sl, (int, float))
                    and isinstance(target, (int, float))
                ):
                    risk = entry_price - sl
                    reward = target - entry_price
                    if risk > 0 and reward > 0:
                        rr_val = round(reward / risk, 2)
                        rr = f"1:{rr_val}"
                    else:
                        rr = ""
                else:
                    rr = ""

                rows.append(
                    [
                        r.get("symbol"),
                        color_signal(r.get("signal")),
                        color_entry(entry),
                        r.get("state"),
                        r.get("score"),
                        last_price,
                        entry_price,
                        target_price,
                        rr,
                        trend_val,
                        pullback_val,
                        trigger_val,
                        reason,
                        sl_val,
                    ]
                )

            return rows

        headers = [
            "Symbol",
            "Signal",
            "Entry",
            "State",
            "Score",
            "Last Price",
            "Entry Px",
            "Target Px",
            "RR",
            "Trend (200D)",
            "Pullback(4H)",
            "Trigger(4H)",
            "Reason",
            "StopLoss",
        ]

        print("\n" + "=" * 80)
        if universe_name:
            print(f"{Colors.CYAN}📦 Universe: {universe_name}{Colors.RESET}")
        print(f"{Colors.CYAN}🔎 Symbols Scanned: {total_symbols}{Colors.RESET}")

        # 🔥 Market context (if available)
        market_info = getattr(service, "market_trend", None)
        if market_info is not None:
            status = "BULLISH" if market_info else "BEARISH"
            color = Colors.GREEN if market_info else Colors.RED
            print(f"{color}📊 Market (NIFTY50): {status}{Colors.RESET}")

        print(f"{Colors.CYAN}⏱️ Execution time: {duration}s{Colors.RESET}")
        print("=" * 80)

        print("\n" + "=" * 80)
        print(f"{Colors.CYAN}🎯 TRADING OPPORTUNITIES{Colors.RESET}")
        print("=" * 80)

        if high_conviction:
            print(f"\n{Colors.GREEN}🔥 HIGH CONVICTION TRADES{Colors.RESET}")
            print(
                tabulate(
                    build_table(high_conviction), headers=headers, tablefmt="pretty"
                )
            )

        if watchlist:
            print(f"\n{Colors.YELLOW}👀 WATCHLIST{Colors.RESET}")
            print(tabulate(build_table(watchlist), headers=headers, tablefmt="pretty"))

        if DEBUG and others:
            print(f"\n{Colors.RED}🧊 LOW QUALITY / NO TRADE{Colors.RESET}")
            print(tabulate(build_table(others), headers=headers, tablefmt="pretty"))

        print("\n" + "=" * 80)
        print(
            f"Total Stocks Processed: {len(results)} | High Conviction: {len(high_conviction)} | Watchlist: {len(watchlist)}"
        )
        print("=" * 80)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("❌ ERROR OCCURRED:", str(e))
