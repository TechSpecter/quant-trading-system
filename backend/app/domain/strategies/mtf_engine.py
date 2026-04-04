import pandas as pd
from typing import Dict, Any


class MTFEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.ind_cfg = config.get("indicators", {})
        self.entry_cfg = config.get("entry", {})
        self.state_cfg = config.get("state", {})
        self.scoring_cfg = config.get(
            "scoring",
            {
                "trend": 40,
                "pullback": 30,
                "trigger": 30,
            },
        )

    # =========================
    # HELPER FUNCTIONS
    # =========================

    def evaluate_rules(self, row: pd.Series, rules) -> bool:
        """Generic rule evaluator from config"""
        for rule in rules:
            left = row.get(rule.get("left"))
            right = row.get(rule.get("right"))

            if left is None or right is None:
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

    def is_bull_trend(self, row: pd.Series) -> bool:
        """Config-driven trend evaluation"""
        trend_rules = self.config.get("trend", {}).get("rules", [])

        if not trend_rules:
            return False

        return self.evaluate_rules(row, trend_rules)

    def is_pullback(self, df: pd.DataFrame, i: int) -> bool:
        row = df.iloc[i]

        ema20 = row["EMA_20"]
        ema50 = row["EMA_50"]
        close = row["close"]
        rsi = row["RSI"]
        vol = row.get("volume", None)
        vol_ma = row.get("VOL_MA", None)

        # Condition 1: EMA20 > EMA50
        if ema20 <= ema50:
            return False

        # Condition 2: Price near EMA zone
        buffer = self.entry_cfg["pullback"]["ema_zone_buffer"]
        lower = ema50 * (1 - buffer)
        upper = ema20 * (1 + buffer)

        if not (lower <= close <= upper):
            return False

        # Condition 3: RSI in pullback range
        rsi_low, rsi_high = self.ind_cfg["rsi"]["pullback_range"]
        if not (rsi_low <= rsi <= rsi_high):
            return False

        # Condition 4: Volume dry-up (optional for indices)
        if vol is not None and vol_ma is not None:
            if vol > vol_ma:
                return False

        return True

    def is_trigger(self, df: pd.DataFrame, i: int) -> bool:
        if i == 0:
            return False

        row = df.iloc[i]
        prev = df.iloc[i - 1]

        # EMA crossover
        cross = prev["EMA_5"] <= prev["EMA_10"] and row["EMA_5"] > row["EMA_10"]

        if not cross:
            return False

        # Volume confirmation
        vol = row.get("volume", None)
        vol_ma = row.get("VOL_MA", None)

        if vol is not None and vol_ma is not None:
            multiplier = self.ind_cfg["volume"]["breakout_multiplier"]
            if vol < vol_ma * multiplier:
                return False

        return True

    # =========================
    # MAIN ENGINE
    # =========================

    def generate_signal(self, mtf_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        MTF logic:
        - Daily (higher TF): trend (Gate 1)
        - Lower TF (e.g. 4H): setup + trigger (Gate 2 & 3)
        """

        print("\n🔍 MTF DEBUG INPUT:")
        for tf, df in mtf_data.items():
            if df is None:
                print(f"   {tf}: None")
            else:
                print(f"   {tf}: rows={len(df)}")

        # Resolve timeframes
        tf_cfg = self.config.get("timeframes", {})
        primary_tf = tf_cfg.get("primary", "D")
        confirm_tf = tf_cfg.get("confirmation", "4H")

        df_primary = mtf_data.get(primary_tf)
        df_confirm = mtf_data.get(confirm_tf)

        # Safety checks
        if df_primary is None or df_primary.empty:
            print("❌ Missing primary timeframe data")
            return {"signal": "NO_DATA", "state": "NO_DATA", "gate_summary": {}}

        confirm_available = df_confirm is not None and not df_confirm.empty

        if not confirm_available:
            print("⚠️ Missing confirmation timeframe data → continuing with trend only")
            last_primary = df_primary.iloc[-1]
            trend_weight = self.scoring_cfg.get("trend", 40)
            gate1_pass = self.is_bull_trend(last_primary)

            sma_col = f"SMA_{self.ind_cfg['sma']['long_term']}"

            score = trend_weight if gate1_pass else 0

            return {
                "signal": "WEAK_BUY" if gate1_pass else "NO_TRADE",
                "score": score,
                "state": "NO_CONFIRM",
                "entry_price": None,
                "stop_loss": None,
                "gate_summary": {
                    "gate_1_trend": {
                        "passed": bool(gate1_pass),
                        "status": "PASS" if gate1_pass else "FAIL",
                        "rule_debug": f"close={last_primary.get('close')} | EMA20={last_primary.get('EMA_20')} | EMA50={last_primary.get('EMA_50')} | SMA200={last_primary.get(sma_col)}",
                        "close": (
                            float(last_primary.get("close", 0))
                            if last_primary.get("close") is not None
                            else None
                        ),
                        "EMA_50": (
                            float(val)
                            if (val := last_primary.get("EMA_50")) is not None
                            else None
                        ),
                        "EMA_20": (
                            float(val)
                            if (val := last_primary.get("EMA_20")) is not None
                            else None
                        ),
                        "SMA_200": (
                            float(val)
                            if sma_col in last_primary
                            and (val := last_primary.get(sma_col)) is not None
                            else None
                        ),
                    },
                    "gate_2_pullback": None,
                    "gate_3_trigger": None,
                },
            }

        # =========================
        # GATE 1 (Trend on higher TF)
        # =========================
        last_primary = df_primary.iloc[-1]
        gate1_pass = self.is_bull_trend(last_primary)

        score = 0
        trend_weight = self.scoring_cfg.get("trend", 40)
        if gate1_pass:
            score += trend_weight

        # =========================
        # GATE 2 & 3 (on lower TF)
        # =========================
        state = "IDLE"
        signal = "HOLD"
        entry_price = None
        stop_loss = None
        target_price = None

        if df_confirm is not None:
            print(f"\n📊 Running lower timeframe logic: rows={len(df_confirm)}")

        if df_confirm is not None:
            for i in range(len(df_confirm)):
                row = df_confirm.iloc[i]

                if not gate1_pass:
                    state = "IDLE"
                    continue

                # Gate 2: Pullback
                if self.is_pullback(df_confirm, i):
                    state = "SETUP"
                    print(f"📍 Pullback detected at index {i}")

                # Gate 3: Trigger
                if state == "SETUP" and self.is_trigger(df_confirm, i):
                    state = "TRIGGERED"
                    signal = "BUY"

                    # 🔧 CONFIG-DRIVEN ENTRY PRICE
                    exec_cfg = self.entry_cfg.get("execution", {})
                    price_source = exec_cfg.get("price_source", "close")
                    buffer_pct = exec_cfg.get("buffer_percent", 0)

                    base_price = (
                        row.get("high") if price_source == "high" else row.get("close")
                    )

                    if base_price is not None:
                        entry_price = base_price * (1 + buffer_pct)
                    else:
                        entry_price = None

                    print(f"🚀 Trigger fired at index {i} entry_price={entry_price}")

                    atr = row.get("ATR", None)

                    risk_cfg = self.config.get("risk", {})
                    atr_cfg = self.ind_cfg.get("atr", {})

                    mode = risk_cfg.get("mode", "fixed_rr")

                    if atr is not None:
                        sl_mult = atr_cfg.get("stop_loss_multiplier", 1.5)
                        tgt_mult = atr_cfg.get("target_multiplier", 3)

                        # 🔻 Stop loss (ATR based)
                        stop_loss = entry_price - (sl_mult * atr)

                        # 🔺 ATR target
                        atr_target = entry_price + (tgt_mult * atr)

                        # 🔺 Structure target (recent swing high)
                        lookback = self.config.get("target", {}).get(
                            "swing_lookback", 20
                        )
                        start_idx = max(0, i - lookback)
                        swing_high = df_confirm.iloc[start_idx : i + 1]["high"].max()

                        # 🔥 CONFIG-DRIVEN TARGET LOGIC
                        risk_cfg = self.config.get("risk", {})
                        fixed_rr_enabled = risk_cfg.get("fixed_rr_enabled", False)
                        rr_ratio = risk_cfg.get("reward_to_risk_ratio", 2)

                        # Calculate risk from SL
                        risk = (
                            entry_price - stop_loss if stop_loss is not None else None
                        )

                        if fixed_rr_enabled and risk is not None and risk > 0:
                            # 🎯 FIXED RR MODE
                            target_price = entry_price + (risk * rr_ratio)
                            print(
                                f"🎯 Fixed RR Target={target_price:.2f} (RR={rr_ratio})"
                            )
                        else:
                            # 🎯 DYNAMIC MODE (ATR + STRUCTURE)
                            if swing_high is not None and swing_high > entry_price:
                                target_price = swing_high
                            else:
                                target_price = atr_target

                        print(
                            f"🎯 ATR Target={atr_target:.2f} | Swing High={swing_high:.2f} | Final Target={target_price:.2f}"
                        )

                    # 🔥 VALIDATION: Ensure logical trade structure
                    if stop_loss is not None and entry_price is not None:
                        if stop_loss >= entry_price:
                            print("⚠️ Invalid SL > Entry, fixing...")
                            stop_loss = entry_price * 0.98

                    if target_price is not None and entry_price is not None:
                        if target_price <= entry_price:
                            print("⚠️ Invalid Target <= Entry, fixing...")
                            target_price = entry_price * 1.04

                    # 🔥 STRICT BUY VALIDATION (never allow invalid structure)
                    if entry_price is not None:
                        if stop_loss is None or stop_loss >= entry_price:
                            print("⚠️ Fixing SL below entry")
                            stop_loss = entry_price * 0.98

                        if target_price is None or target_price <= entry_price:
                            print("⚠️ Fixing Target above entry")
                            target_price = entry_price * 1.04

                    # 🔥 RISK-REWARD FILTER (CONFIG DRIVEN)
                    rr_cfg = self.config.get("risk", {})
                    min_rr = rr_cfg.get("min_rr", 1.5)

                    if (
                        entry_price is not None
                        and stop_loss is not None
                        and target_price is not None
                    ):
                        risk = entry_price - stop_loss
                        reward = target_price - entry_price

                        if risk > 0:
                            rr = reward / risk
                            print(f"📊 RR calculated: {rr:.2f}")

                            if rr < min_rr:
                                print(
                                    f"❌ Skipping trade due to low RR ({rr:.2f} < {min_rr})"
                                )
                                signal = "NO_TRADE"
                                entry_signal = "SKIP"
                                trigger_reason = f"Low RR ({rr:.2f})"
                                entry_price = None
                                target_price = None
                                stop_loss = None

        # =========================
        # DEBUG SUMMARY
        # =========================
        if not confirm_available or df_confirm is None or len(df_confirm) == 0:
            last_confirm = None
        else:
            last_confirm = df_confirm.iloc[-1]

        gate2_pass = False
        gate3_pass = False

        if confirm_available and df_confirm is not None and len(df_confirm) > 0:
            last_index = len(df_confirm) - 1
            try:
                gate2_pass = bool(self.is_pullback(df_confirm, last_index))
            except Exception:
                gate2_pass = False

            try:
                gate3_pass = bool(self.is_trigger(df_confirm, last_index))
            except Exception:
                gate3_pass = False

        vol = None
        vol_ma = None

        if last_confirm is not None:
            vol = last_confirm.get("volume", None)
            vol_ma = last_confirm.get("VOL_MA", None)

        vol_condition = None
        if vol is not None and vol_ma is not None:
            vol_condition = vol <= vol_ma

        volume_spike = None
        if vol is not None and vol_ma is not None:
            multiplier = self.ind_cfg["volume"]["breakout_multiplier"]
            volume_spike = vol >= vol_ma * multiplier

        pullback_weight = self.scoring_cfg.get("pullback", 30)
        if gate2_pass:
            score += pullback_weight

        trigger_weight = self.scoring_cfg.get("trigger", 30)
        if gate3_pass:
            score += trigger_weight

        sma_col = f"SMA_{self.ind_cfg['sma']['long_term']}"

        max_score = (
            self.scoring_cfg.get("trend", 40)
            + self.scoring_cfg.get("pullback", 30)
            + self.scoring_cfg.get("trigger", 30)
        )

        # Map score to signal
        if score >= 0.8 * max_score:
            signal = "STRONG_BUY"
        elif score >= 0.5 * max_score:
            signal = "BUY"
        elif score >= self.scoring_cfg.get("trend", 40):
            signal = "WEAK_BUY"
        else:
            signal = "NO_TRADE"

        # =========================
        # ENTRY SIGNAL & REASONS
        # =========================
        entry_signal = "WATCH"
        trigger_reason = None

        if gate1_pass and gate2_pass and gate3_pass:
            entry_signal = "BUY_NOW"
            trigger_reason = "EMA5 crossed above EMA10 with volume"
        elif gate1_pass and gate2_pass:
            entry_signal = "WAIT"
            trigger_reason = "Pullback detected, waiting for trigger"
        elif gate1_pass:
            entry_signal = "WATCH"
            trigger_reason = "Uptrend only, no pullback"
        else:
            entry_signal = "AVOID"
            trigger_reason = "No bullish structure"

        target_price = None if "target_price" not in locals() else target_price

        # =========================
        # FALLBACK RISK CALCULATION (ONLY FOR VALID CONTEXT)
        # =========================
        if stop_loss is None and last_confirm is not None and state != "TRIGGERED":
            atr_val = last_confirm.get("ATR", None)
            close_val = last_confirm.get("close", None)

            risk_cfg = self.config.get("risk", {})
            atr_cfg = self.ind_cfg.get("atr", {})
            mode = risk_cfg.get("mode", "fixed_rr")

            if atr_val is not None and close_val is not None:
                sl_mult = atr_cfg.get("stop_loss_multiplier", 1.5)
                stop_loss = close_val - (sl_mult * atr_val)

                tgt_mult = atr_cfg.get("target_multiplier", 3)

                atr_target = close_val + (tgt_mult * atr_val)

                lookback = self.config.get("target", {}).get("swing_lookback", 20)
                if df_confirm is not None and not df_confirm.empty:
                    start_idx = max(0, len(df_confirm) - lookback)
                    swing_high = df_confirm.iloc[start_idx:]["high"].max()
                else:
                    swing_high = None

                if swing_high is not None and swing_high > close_val:
                    target_price = swing_high
                else:
                    target_price = atr_target

                # 🔥 STRICT VALIDATION
                if stop_loss >= close_val:
                    stop_loss = close_val * 0.98

                if target_price <= close_val:
                    target_price = close_val * 1.04

        # Ensure SL/Target always aligned with BUY logic
        if entry_price is not None:
            if stop_loss is not None and stop_loss >= entry_price:
                stop_loss = entry_price * 0.98
            if target_price is not None and target_price <= entry_price:
                target_price = entry_price * 1.04

        # =========================
        # FINAL SANITY CHECK (GLOBAL)
        # =========================
        if (
            entry_price is not None
            and stop_loss is not None
            and target_price is not None
        ):
            if not (entry_price > stop_loss and target_price > entry_price):
                print("⚠️ Final adjustment of SL/Target")
                stop_loss = entry_price * 0.98
                target_price = entry_price * 1.04

        # Ensure entry_price is set if missing but last_confirm is available
        if entry_price is None and last_confirm is not None:
            entry_price = last_confirm.get("close", None)

        # 🔥 GLOBAL RISK-REWARD FILTER (FINAL SAFETY)
        rr_cfg = self.config.get("risk", {})
        min_rr = rr_cfg.get("min_rr", 1.5)

        if (
            entry_price is not None
            and stop_loss is not None
            and target_price is not None
        ):
            risk = entry_price - stop_loss
            reward = target_price - entry_price

            if risk > 0:
                rr = reward / risk
                print(f"📊 Final RR check: {rr:.2f}")

                if rr < min_rr:
                    print(f"❌ Final RR filter triggered: {rr:.2f} < {min_rr}")
                    signal = "NO_TRADE"
                    entry_signal = "SKIP"
                    trigger_reason = f"Low RR ({rr:.2f})"
                    entry_price = None
                    target_price = None
                    stop_loss = None

        return {
            "signal": signal,
            "entry_signal": entry_signal,
            "trigger_reason": trigger_reason,
            "score": score,
            "state": state,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "target_price": target_price,
            "gate_summary": {
                "gate_1_trend": {
                    "passed": bool(gate1_pass),
                    "status": "PASS" if gate1_pass else "FAIL",
                    "rule_debug": f"close={last_primary.get('close')} | EMA20={last_primary.get('EMA_20')} | EMA50={last_primary.get('EMA_50')} | SMA200={last_primary.get(sma_col)}",
                    "close": (
                        float(last_primary.get("close", 0))
                        if last_primary.get("close") is not None
                        else None
                    ),
                    "EMA_50": (
                        float(val)
                        if (val := last_primary.get("EMA_50")) is not None
                        else None
                    ),
                    "EMA_20": (
                        float(val)
                        if (val := last_primary.get("EMA_20")) is not None
                        else None
                    ),
                    "SMA_200": (
                        float(val)
                        if sma_col in last_primary
                        and (val := last_primary.get(sma_col)) is not None
                        else None
                    ),
                },
                "gate_2_pullback": {
                    "passed": bool(gate2_pass),
                    "status": "PASS" if gate2_pass else "FAIL",
                    "ema_20": (
                        float(val)
                        if last_confirm is not None
                        and (val := last_confirm.get("EMA_20")) is not None
                        else None
                    ),
                    "ema_50": (
                        float(val)
                        if last_confirm is not None
                        and (val := last_confirm.get("EMA_50")) is not None
                        else None
                    ),
                    "rsi": (
                        float(val)
                        if last_confirm is not None
                        and (val := last_confirm.get("RSI")) is not None
                        else None
                    ),
                    "volume_condition": (
                        "PASS"
                        if vol_condition is True
                        else "FAIL" if vol_condition is False else "NA"
                    ),
                },
                "gate_3_trigger": {
                    "passed": bool(gate3_pass),
                    "status": "PASS" if gate3_pass else "FAIL",
                    "ema_5": (
                        float(val)
                        if last_confirm is not None
                        and (val := last_confirm.get("EMA_5")) is not None
                        else None
                    ),
                    "ema_10": (
                        float(val)
                        if last_confirm is not None
                        and (val := last_confirm.get("EMA_10")) is not None
                        else None
                    ),
                    "volume_spike": (
                        "PASS"
                        if volume_spike is True
                        else "FAIL" if volume_spike is False else "NA"
                    ),
                },
            },
        }
