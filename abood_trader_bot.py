import asyncio
from collections import deque
import csv
import html
import inspect
import json
import math
import os
import statistics
import sys
import time
from datetime import datetime, timezone, timedelta
import aiohttp
from aiohttp import web
import joblib
import lightgbm as lgb
import numpy as np
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier, VotingClassifier
import websockets

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# ==========================================
# 1. إعدادات الربط والتوقيت (عدّلها بسهولة من هنا)
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "ضع_توكن_بوت_تليجرام_هنا")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "ضع_رقم_الآيدي_الخاص_بك_هنا")
CHANNEL_ID = os.getenv("CHANNEL_ID", "ضع_اي_دي_القناة_هنا")

# مفتاح Gemini API المجاني (اختياري — أضفه في Render ليعمل كمدقق ذكي مزدوج مجانًا)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

DEFAULT_LEAD_SECONDS = 0.1
SERVER_CLOCK_OFFSET = 0.75
MARTINGALE_OFFSET = 0.05

DEFAULT_AUTH_FRAME = os.getenv(
    "PO_AUTH_FRAME",
    '42["auth",{"session":"et97hpjdhses78itun3rk2peld","isDemo":1,"uid":142271281,"platform":2,"isFastHistory":true,"isOptimized":true}]'
)

DEFAULT_LIVE_AUTH_FRAME = os.getenv("PO_LIVE_AUTH_FRAME", "")

ACCOUNTS_FILE = "accounts.json"
STATE_FILE = "bot_state.json"
AI_DATASET_FILE = "ai_ticks_dataset.csv"
AI_MODEL_FILE = "abood_ensemble_model.joblib"

FALLBACK_CURRENCIES = [
    "EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "AUDUSD_otc", "USDCAD_otc",
    "USDCHF_otc", "NZDUSD_otc", "EURJPY_otc", "GBPJPY_otc", "EURGBP_otc",
    "AUDCAD_otc", "AUDJPY_otc", "CADJPY_otc", "CHFJPY_otc", "EURAUD_otc",
    "EURCAD_otc", "GBPAUD_otc", "GBPCAD_otc", "USDINR_otc", "USDBRL_otc",
    "USDCOP_otc", "USDMXN_otc", "USDZAR_otc", "USDSGD_otc"
]

FALLBACK_CRYPTO = [
    "BTCUSD_otc", "ETHUSD_otc", "LTCUSD_otc", "XRPUSD_otc", "SOLUSD_otc",
    "BNBUSD_otc", "DOGEUSD_otc", "ADAUSD_otc", "DOTUSD_otc", "AVAXUSD_otc",
    "LINKUSD_otc", "MATICUSD_otc", "TONUSD_otc", "TRON_otc"
]

FALLBACK_STOCKS = [
    "#AAPL_otc", "#MSFT_otc", "#TSLA_otc", "#AMZN_otc", "#GOOGL_otc",
    "#META_otc", "#NVDA_otc", "#NFLX_otc", "#INTC_otc", "#AMD_otc",
    "#BA_otc", "#DIS_otc", "#JPM_otc", "#V_otc", "#MCD_otc", "#KO_otc", "#BABA_otc"
]

SCAN_INTERVAL_PER_PAIR = 3
TRADE_DURATION = 60
TZ_UTC3 = timezone(timedelta(hours=3))
# ==========================================


def log(tag: str, msg: str):
    now = datetime.now(tz=TZ_UTC3).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}] [{tag}] {msg}", flush=True)


def to_superscript(num: int) -> str:
    sup_map = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")
    return str(num).translate(sup_map)


def normalize_ts(raw_ts) -> float:
    ts = float(raw_ts)
    if ts > 10_000_000_000:
        ts /= 1000.0
    return ts


def candle_color(open_price: float, close_price: float) -> str:
    return "GREEN" if close_price >= open_price else "RED"


def candle_body_size(candle: dict) -> float:
    return abs(float(candle["close"]) - float(candle["open"]))


def color_label(color: str) -> str:
    if color == "GREEN":
        return "🟢 أخضر"
    elif color == "RED":
        return "🔴 أحمر"
    return "⚪ غير متاح"


def make_progress_bar(count: int, total: int, length: int = 10) -> str:
    if total <= 0 or count <= 0:
        return "░" * length
    filled = max(1, round((count / total) * length)) if count > 0 else 0
    filled = min(length, filled)
    return "█" * filled + "░" * (length - filled)


def format_money(val: float) -> str:
    return f"{int(val):,}" if float(val).is_integer() else f"{val:,.2f}"


def parse_auth_frame(frame: str) -> dict | None:
    try:
        clean = frame.strip()
        if not clean.startswith('42["auth",'):
            return None
        parsed = json.loads(clean[2:])
        if isinstance(parsed, list) and len(parsed) >= 2 and isinstance(parsed[1], dict):
            return parsed[1]
    except Exception:
        pass
    return None


def make_auth_frame(session_str: str, uid_val: str | int, is_demo: bool) -> str:
    try:
        uid_clean = int(uid_val)
    except (ValueError, TypeError):
        uid_clean = str(uid_val)
    payload = {
        "session": str(session_str).strip(),
        "isDemo": 1 if is_demo else 0,
        "uid": uid_clean,
        "platform": 2,
        "isFastHistory": True,
        "isOptimized": True
    }
    return f'42["auth",{json.dumps(payload, separators=(",", ":"))}]'


def build_auth_frame_with_mode(frame: str, is_demo: bool) -> str:
    payload = parse_auth_frame(frame)
    if payload is None:
        return frame
    payload["isDemo"] = 1 if is_demo else 0
    return f'42["auth",{json.dumps(payload, separators=(",", ":"))}]'


# ==========================================
# دوال حساب المؤشرات الفنية والاتجاه
# ==========================================
def calc_ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = (v * k) + (ema * (1.0 - k))
    return ema


def calc_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [max(-d, 0.0) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calc_bollinger(closes: list[float], period: int = 20, mult: float = 2.0) -> tuple[float, float, float] | None:
    if len(closes) < period:
        return None
    window = closes[-period:]
    mid = sum(window) / period
    variance = sum((x - mid) ** 2 for x in window) / period
    std = variance ** 0.5
    return (mid - mult * std), mid, (mid + mult * std)


def calc_stochastic(candles_list: list[dict], k_period: int = 5, smooth: int = 3) -> float | None:
    if len(candles_list) < k_period + smooth - 1:
        return None
    raw_k = []
    for i in range(len(candles_list) - smooth + 1, len(candles_list) + 1):
        window = candles_list[i - k_period:i]
        hh = max(c.get("high", max(c["open"], c["close"])) for c in window)
        ll = min(c.get("low", min(c["open"], c["close"])) for c in window)
        cc = window[-1]["close"]
        if hh == ll:
            raw_k.append(50.0)
        else:
            raw_k.append(((cc - ll) / (hh - ll)) * 100.0)
    return sum(raw_k) / len(raw_k)


def calc_atr(candles_list: list[dict], period: int = 14) -> float | None:
    if len(candles_list) < period + 1:
        return None
    trs = []
    for i in range(-period, 0):
        curr = candles_list[i]
        prev = candles_list[i - 1]
        h = curr.get("high", max(curr["open"], curr["close"]))
        l = curr.get("low", min(curr["open"], curr["close"]))
        pc = prev["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs) / period


# ==========================================
# محرك الذكاء الاصطناعي الهجين المتطور (Ensemble ML + Trend Guard + Gemini AI)
# ==========================================
class HybridEnsembleAIEngine:
    """
    يدمج 3 طبقات ذكاء اصطناعي:
    1) حارس الاتجاه ومنع عكس الترند الصاروخي (Trend & Exhaustion Guard).
    2) نموذج Ensemble ثلاثي (LightGBM + RandomForest + ExtraTrees) مُدرَّب على بيانات M1.
    3) مدقق Gemini 2.5 Flash المجاني لتأكيد الإشارات عالية الثقة.
    """
    CSV_HEADERS = [
        "abs_z", "rsi_live", "rsi_7", "velocity_ratio", "momentum_5",
        "range_ratio", "ema_dist_atr", "wick_reject", "dir_num", "hour_utc", "label"
    ]

    def __init__(self, min_confidence: float = 0.82):
        self.min_confidence = min_confidence
        self.tick_windows: dict[str, deque] = {}
        self.pair_cooldown: dict[str, float] = {}
        self.shadow_cooldown: dict[str, float] = {}
        self.pending_samples: list[dict] = []
        self.model = None
        self.samples_count = 0
        self.gemini_enabled = bool(GEMINI_API_KEY)
        self.gemini_calls_today = 0
        self.gemini_last_verdict = "غير نشط"
        self._init_dataset_file()
        self._load_trained_model()

    def _init_dataset_file(self):
        if not os.path.exists(AI_DATASET_FILE):
            try:
                with open(AI_DATASET_FILE, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerow(self.CSV_HEADERS)
            except Exception:
                pass
        else:
            try:
                with open(AI_DATASET_FILE, "r", encoding="utf-8") as f:
                    first_line = f.readline().strip()
                    if "ema_dist_atr" not in first_line:
                        # ترقية ملف البيانات القديم تلقائيًا إلى الهيكل الجديد ذي الـ 10 ميزات
                        with open(AI_DATASET_FILE, "w", newline="", encoding="utf-8") as fw:
                            csv.writer(fw).writerow(self.CSV_HEADERS)
                        self.samples_count = 0
                    else:
                        self.samples_count = max(0, sum(1 for _ in f))
            except Exception:
                self.samples_count = 0

    def _load_trained_model(self):
        if os.path.exists(AI_MODEL_FILE):
            try:
                self.model = joblib.load(AI_MODEL_FILE)
                log("AI_ENSEMBLE", f"تم تحميل نموذج Ensemble الثلاثي بنجاح ✅ (عينات: {self.samples_count})")
            except Exception as e:
                log("AI_WARN", f"تعذر تحميل النموذج: {e}")
                self.model = None

    def reset(self):
        self.tick_windows.clear()
        self.pair_cooldown.clear()

    def resolve_shadow_samples(self, latest_prices: dict, now_ts: float):
        if not self.pending_samples:
            return
        still_waiting = []
        resolved_rows = []
        for smp in self.pending_samples:
            if now_ts >= smp["eval_ts"]:
                curr_p = latest_prices.get(smp["asset"])
                if curr_p is not None and curr_p != smp["entry_price"]:
                    is_win = (curr_p > smp["entry_price"]) if smp["dir"] == "call" else (curr_p < smp["entry_price"])
                    resolved_rows.append(smp["features"] + [1 if is_win else 0])
            else:
                still_waiting.append(smp)

        self.pending_samples = still_waiting
        if resolved_rows:
            try:
                with open(AI_DATASET_FILE, "a", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerows(resolved_rows)
                self.samples_count += len(resolved_rows)
            except Exception:
                pass

    def train_model_now(self) -> tuple[bool, str]:
        if not os.path.exists(AI_DATASET_FILE):
            return False, "لا يوجد ملف بيانات بعد."
        X, y = [], []
        try:
            with open(AI_DATASET_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if "ema_dist_atr" not in row or row["ema_dist_atr"] is None:
                        continue
                    X.append([
                        float(row["abs_z"]),
                        float(row["rsi_live"]),
                        float(row["rsi_7"]),
                        float(row["velocity_ratio"]),
                        float(row["momentum_5"]),
                        float(row["range_ratio"]),
                        float(row["ema_dist_atr"]),
                        float(row["wick_reject"]),
                        float(row["dir_num"]),
                        float(row["hour_utc"]),
                    ])
                    y.append(int(row["label"]))
        except Exception as e:
            return False, f"خطأ في قراءة البيانات: {e}"

        if len(X) < 80:
            return False, f"عدد العينات المسجلة ({len(X)}) غير كافٍ للتدريب عالي الدقة (المطلوب 80 عينة على الأقل)."

        if len(set(y)) < 2:
            return False, "العينات المسجلة تحتوي على فئة واحدة فقط؛ انتظر جمع عينات متنوعة."

        X_arr = np.array(X, dtype=np.float32)
        y_arr = np.array(y, dtype=np.int32)

        # دمج 3 خوارزميات قوية بالتصويت الموزون (Soft Voting Ensemble)
        m_lgb = lgb.LGBMClassifier(
            n_estimators=120, learning_rate=0.04, num_leaves=15,
            subsample_for_bin=50, min_child_samples=8, random_state=42, verbose=-1
        )
        m_rf = RandomForestClassifier(
            n_estimators=120, max_depth=6, min_samples_leaf=5, random_state=42, n_jobs=-1
        )
        m_et = ExtraTreesClassifier(
            n_estimators=120, max_depth=6, min_samples_leaf=5, random_state=42, n_jobs=-1
        )

        ensemble = VotingClassifier(
            estimators=[("lgb", m_lgb), ("rf", m_rf), ("et", m_et)],
            voting="soft",
            weights=[2, 1, 1]
        )
        ensemble.fit(X_arr, y_arr)
        joblib.dump(ensemble, AI_MODEL_FILE)
        self.model = ensemble
        acc = float(np.mean(ensemble.predict(X_arr) == y_arr)) * 100.0
        return True, f"تم تدريب نموذج Ensemble الثلاثي (LightGBM + RF + ET) على {len(X)} عينة! (الدقة: {acc:.1f}%)"

    def evaluate_tick(self, asset: str, price: float, now_ts: float, pair_candles: dict) -> tuple[str | None, float, str, dict]:
        if asset not in self.tick_windows:
            self.tick_windows[asset] = deque(maxlen=150)

        win = self.tick_windows[asset]
        win.append((now_ts, price))

        if len(win) < 50:
            return None, 0.0, "", {}

        sorted_mins = sorted(pair_candles.keys())
        if len(sorted_mins) < 22:
            return None, 0.0, "", {}

        ordered_c = [pair_candles[m] for m in sorted_mins[-30:]]
        closes_c = [c["close"] for c in ordered_c]
        ema20 = calc_ema(closes_c, 20)
        atr14 = calc_atr(ordered_c, 14)
        if ema20 is None or atr14 is None or atr14 <= 1e-9:
            return None, 0.0, "", {}

        # قياس بُعد السعر عن المتوسط العام EMA20 بوحدات ATR (لمنع الدخول ضد ترند منفجر)
        ema_dist_atr = (price - ema20) / atr14

        prices = [p for _, p in win]
        times = [t for t, _ in win]

        recent_p = prices[-45:]
        mean_p = sum(recent_p) / len(recent_p)
        std_p = math.sqrt(sum((x - mean_p) ** 2 for x in recent_p) / len(recent_p))
        if std_p <= 1e-9:
            return None, 0.0, "", {}
        z_score = (price - mean_p) / std_p

        ticks_5s = sum(1 for t in times if (now_ts - t) <= 5.0)
        ticks_30s = sum(1 for t in times if (now_ts - t) <= 30.0)
        velocity_ratio = (ticks_5s / 5.0) / max((ticks_30s / 30.0), 0.1)

        # RSI(14) و RSI(7) على التيكات الحية
        d14 = [prices[i] - prices[i - 1] for i in range(-14, 0)]
        g14 = sum(d for d in d14 if d > 0) / 14.0
        l14 = sum(-d for d in d14 if d < 0) / 14.0
        rsi_live = 100.0 if l14 == 0 else 100.0 - (100.0 / (1.0 + (g14 / l14)))

        d7 = d14[-7:]
        g7 = sum(d for d in d7 if d > 0) / 7.0
        l7 = sum(-d for d in d7 if d < 0) / 7.0
        rsi_7 = 100.0 if l7 == 0 else 100.0 - (100.0 / (1.0 + (g7 / l7)))

        momentum_5 = (price - prices[-6]) / std_p
        range_45 = max(recent_p) - min(recent_p)
        range_ratio = abs(price - mean_p) / max(range_45, 1e-9)

        # فحص الرفض السعري (Wick Rejection) في الشمعة الحالية
        last_c = ordered_c[-1]
        c_range = max(last_c["high"] - last_c["low"], 1e-9)
        upper_wick_ratio = (last_c["high"] - max(last_c["open"], last_c["close"])) / c_range
        lower_wick_ratio = (min(last_c["open"], last_c["close"]) - last_c["low"]) / c_range

        p_prev1, p_prev2, p_prev3 = prices[-2], prices[-3], prices[-4]

        # شروط أكثر صرامة: انحراف حاد + تشبع مزدوج + كسر 3 تيكات + وجود ذيل رفض سعري
        if (
            z_score >= 2.25
            and rsi_live >= 74.0
            and rsi_7 >= 78.0
            and price < p_prev1 < p_prev2
            and p_prev2 <= p_prev3
            and ema_dist_atr <= 2.8  # لا تبع إذا كان السوق في ترند صاعد منفجر فوق 2.8 ATR
        ):
            direction = "put"
            wick_reject = upper_wick_ratio
        elif (
            z_score <= -2.25
            and rsi_live <= 26.0
            and rsi_7 <= 22.0
            and price > p_prev1 > p_prev2
            and p_prev2 >= p_prev3
            and ema_dist_atr >= -2.8  # لا تشترِ إذا كان السوق في انهيار حاد تحت -2.8 ATR
        ):
            direction = "call"
            wick_reject = lower_wick_ratio
        else:
            return None, 0.0, "", {}

        dir_num = 1.0 if direction == "call" else -1.0
        hour_utc = float(datetime.fromtimestamp(now_ts, tz=timezone.utc).hour)
        feat_row = [
            round(abs(z_score), 4),
            round(rsi_live, 2),
            round(rsi_7, 2),
            round(velocity_ratio, 3),
            round(momentum_5, 4),
            round(range_ratio, 4),
            round(ema_dist_atr, 4),
            round(wick_reject, 4),
            dir_num,
            hour_utc
        ]

        # تسجيل عينة صامتة في الخلفية كل 40 ثانية
        if now_ts >= self.shadow_cooldown.get(asset, 0.0) and len(self.pending_samples) < 500:
            self.shadow_cooldown[asset] = now_ts + 40.0
            self.pending_samples.append({
                "asset": asset,
                "dir": direction,
                "entry_price": price,
                "eval_ts": now_ts + 60.0,
                "features": feat_row
            })

        if now_ts < self.pair_cooldown.get(asset, 0.0):
            return None, 0.0, "", {}

        if self.model is not None:
            engine_name = "Ensemble ML"
            X_in = np.array([feat_row], dtype=np.float32)
            confidence = float(self.model.predict_proba(X_in)[0][1])
        else:
            engine_name = "AI Guard"
            z_norm = min(abs(z_score) / 3.2, 1.0)
            rsi_ext = ((rsi_live - 50.0) / 50.0) if direction == "put" else ((50.0 - rsi_live) / 50.0)
            wick_bonus = min(wick_reject * 1.5, 1.0)
            vel_norm = min(velocity_ratio / 2.5, 1.0)
            confidence = (0.35 * z_norm) + (0.30 * rsi_ext) + (0.20 * wick_bonus) + (0.15 * vel_norm)

        meta = {
            "z_score": round(z_score, 2),
            "rsi_14": round(rsi_live, 1),
            "rsi_7": round(rsi_7, 1),
            "ema_dist_atr": round(ema_dist_atr, 2),
            "wick_reject_pct": round(wick_reject * 100, 1),
            "velocity": round(velocity_ratio, 2),
            "last_3_candles": [
                {"o": c["open"], "h": c["high"], "l": c["low"], "c": c["close"]}
                for c in ordered_c[-3:]
            ]
        }

        if confidence >= self.min_confidence:
            self.pair_cooldown[asset] = now_ts + 70.0
            return direction, round(confidence, 3), engine_name, meta

        return None, confidence, engine_name, meta


class AboodControlBot:
    def __init__(self):
        self.is_running = False
        self.base_amount = 10.0
        self.martingale_enabled = True
        self.global_steps = 6
        self.pair_steps = 4
        self.min_payout = 90
        self.entry_lead_seconds = DEFAULT_LEAD_SECONDS

        # الاستراتيجية النشطة: "CHESS" أو "INDICATORS" أو "AI"
        self.strategy_mode = "CHESS"
        self.ai_min_confidence = 0.82
        self.ai_scorer = HybridEnsembleAIEngine(min_confidence=self.ai_min_confidence)
        self.ind_strategy_cooldown = {}

        self.fast_mode = False
        self.strict_chess_enabled = False
        self.strict_body_ratio = 0.25
        self.sixth_candle_enabled = False

        self.max_locked_pairs = 1
        self.max_concurrent_trades = 1
        self.locked_candidates = []

        self.limits_enabled = False
        self.profit_limit = 50.0
        self.loss_limit = 50.0
        self.limit_start_balance = None

        self.indicators_enabled = False
        self.ind_bb_enabled = True
        self.ind_bb_zone = 0.70
        self.ind_rsi_enabled = True
        self.ind_rsi_period = 14
        self.ind_rsi_overbought = 62.0
        self.ind_rsi_oversold = 38.0
        self.ind_wick_enabled = True
        self.ind_stoch_enabled = False
        self.ind_stoch_overbought = 75.0
        self.ind_stoch_oversold = 25.0
        self.ind_atr_enabled = False
        self.last_ind_skip_log = {}

        self.accounts = []
        self.active_account_idx = 0
        self.is_demo_mode = True
        self.auth_frame = DEFAULT_AUTH_FRAME
        self.temp_new_acc_name = ""
        self.temp_new_acc_demo_frame = ""
        self._load_accounts_storage()

        self.boost_enabled = False
        self.boost_mode = "CAPITAL"
        self.boost_increment = 10.0
        self.boost_target_wins = 5
        self.boost_wins_done = 0
        self.effective_base_amount = 10.0
        self.in_boost_recovery = False
        self.peak_balance = None
        self.current_cycle_spent = 0.0

        self.continuation_enabled = False
        self.scan_scope = "CURRENCIES"

        self.assets_by_cat = {
            "CURRENCIES": list(FALLBACK_CURRENCIES),
            "CRYPTO": list(FALLBACK_CRYPTO),
            "STOCKS": list(FALLBACK_STOCKS),
        }
        self.asset_display_names = {}
        self.awaiting_input = None

        self.candles = {}
        self.latest_prices = {}
        self.payouts = {}
        self.open_assets = set()
        self.subscribed_pairs = set()
        self.last_logged_state = {}
        self.pending_binary_event = None
        self.active_symbol = FALLBACK_CURRENCIES[0]
        self.authenticated = False
        self.ws = None
        self.http_session = None
        self.req_index = 100
        self.account_balance = None
        self.server_hour_shift = 0.0

        self.channel_queue = asyncio.Queue()
        self.signal_seq = 0
        self.signal_msg_ids = {}

        self.mode = "SCANNING"
        self.active_trades = []
        self.trade_seq = 0
        self.global_step_index = 0
        self.pending_martingale_steps = []

        self.stats_date = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
        self.daily_records = []
        self.total_turnover = 0.0
        self._load_bot_state()

    # ==========================================
    # حفظ واستعادة الحالة
    # ==========================================
    def _load_bot_state(self):
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                st = json.load(f)
            if isinstance(st, dict):
                today_str = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
                if st.get("stats_date") == today_str:
                    self.stats_date = today_str
                    self.daily_records = list(st.get("daily_records", []))
                    self.total_turnover = float(st.get("total_turnover", 0.0))
                self.limits_enabled = bool(st.get("limits_enabled", False))
                self.profit_limit = float(st.get("profit_limit", 50.0))
                self.loss_limit = float(st.get("loss_limit", 50.0))
                self.strict_chess_enabled = bool(st.get("strict_chess_enabled", False))
                self.sixth_candle_enabled = bool(st.get("sixth_candle_enabled", False))
                saved_strat = str(st.get("strategy_mode", "CHESS")).upper()
                self.strategy_mode = saved_strat if saved_strat in ("CHESS", "INDICATORS", "AI") else "CHESS"
                self.ai_min_confidence = float(st.get("ai_min_confidence", 0.82))
                self.ai_scorer.min_confidence = self.ai_min_confidence
        except Exception as e:
            log("STATE_WARN", f"تعذر قراءة ملف الحالة: {e}")

    def _save_bot_state(self):
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "stats_date": self.stats_date,
                    "daily_records": self.daily_records,
                    "total_turnover": self.total_turnover,
                    "limits_enabled": self.limits_enabled,
                    "profit_limit": self.profit_limit,
                    "loss_limit": self.loss_limit,
                    "strict_chess_enabled": self.strict_chess_enabled,
                    "sixth_candle_enabled": self.sixth_candle_enabled,
                    "strategy_mode": self.strategy_mode,
                    "ai_min_confidence": self.ai_min_confidence,
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log("STATE_WARN", f"تعذر حفظ ملف الحالة: {e}")

    # ==========================================
    # فحص حدود الربح والخسارة
    # ==========================================
    def capture_start_balance_on_run(self):
        if self.account_balance is not None:
            bal = float(self.account_balance)
            self.limit_start_balance = bal
            if self.peak_balance is None or bal > self.peak_balance:
                self.peak_balance = bal
        else:
            self.limit_start_balance = None

    def update_balance_and_peak(self, new_bal: float) -> bool:
        self.account_balance = float(new_bal)

        if self.peak_balance is None:
            self.peak_balance = float(new_bal)
        elif float(new_bal) >= self.peak_balance:
            self.peak_balance = float(new_bal)
            if self.boost_enabled and self.in_boost_recovery and self.boost_mode == "CAPITAL":
                log("BOOST_DONE", f"🎉 عاد الرصيد إلى القمة (${format_money(self.peak_balance)})!")
                asyncio.create_task(self.send_admin(
                    f"🎉 <b>عاد رصيد الحساب الفعلي إلى القمة (${format_money(self.peak_balance)})!</b>\n"
                    f"💵 عاد المبلغ الأساسي تلقائيًا إلى <b>${self.base_amount:g}</b>."
                ))
                self.reset_boost_state(reset_peak=False)

        return self.check_and_enforce_limits(float(new_bal), allow_loss_check=(len(self.active_trades) == 0))

    def check_and_enforce_limits(self, current_bal: float, allow_loss_check: bool = True) -> bool:
        if not self.is_running or not self.limits_enabled:
            return False

        if self.limit_start_balance is None:
            self.limit_start_balance = float(current_bal)
            return False

        start_b = self.limit_start_balance
        diff = float(current_bal) - start_b

        if self.profit_limit > 0 and diff >= (self.profit_limit - 1e-6):
            self.is_running = False
            self.full_reset_to_scanning("الوصول إلى حد الربح")
            self.limit_start_balance = None
            log("LIMIT_PROFIT", f"🎯 تم الوصول إلى حد الربح! (+${format_money(diff)})")
            asyncio.create_task(self.send_admin(
                f"🎯 <b>تم الوصول إلى حد الربح — تم إيقاف التداول وإلغاء الصفقات المجهزة فورًا ✅</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>رصيد بدء التداول:</b> <code>${format_money(start_b)}</code>\n"
                f"💵 <b>الرصيد الحالي:</b> <code>${format_money(current_bal)}</code>\n"
                f"📈 <b>الربح المحقق:</b> <code>+${format_money(diff)}</code>",
                self.build_main_keyboard()
            ))
            return True

        if allow_loss_check and not self.active_trades:
            if self.loss_limit > 0 and (-diff) >= (self.loss_limit - 1e-6):
                self.is_running = False
                self.full_reset_to_scanning("الوصول إلى حد الخسارة")
                self.limit_start_balance = None
                log("LIMIT_LOSS", f"🛑 تم الوصول إلى حد الخسارة! (-${format_money(abs(diff))})")
                asyncio.create_task(self.send_admin(
                    f"🛑 <b>تم الوصول إلى حد الخسارة — تم إيقاف التداول والصفقات فورًا ⚠️</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"💰 <b>رصيد بدء التداول:</b> <code>${format_money(start_b)}</code>\n"
                    f"💵 <b>الرصيد الحالي:</b> <code>${format_money(current_bal)}</code>\n"
                    f"📉 <b>الخسارة المسجلة:</b> <code>-${format_money(abs(diff))}</code>",
                    self.build_main_keyboard()
                ))
                return True

        return False

    # ==========================================
    # فحص «الشطرنج الشديد» والمؤشرات
    # ==========================================
    def check_strict_chess_candles(self, asset: str, minutes_tuple: tuple) -> tuple[bool, str]:
        if not self.strict_chess_enabled:
            return True, "عادي"

        pair_candles = self.candles.get(asset, {})
        sorted_mins = sorted(pair_candles.keys())
        if len(sorted_mins) < 8:
            return False, "شموع تاريخية غير كافية"

        prev_mins = [m for m in sorted_mins if m < minutes_tuple[0]][-20:]
        if not prev_mins:
            prev_mins = sorted_mins[:-len(minutes_tuple)][-15:]

        prev_bodies = [candle_body_size(pair_candles[m]) for m in prev_mins if candle_body_size(pair_candles[m]) > 0]
        if not prev_bodies:
            return False, "لا توجد شموع مرجعية"

        median_body = statistics.median(prev_bodies)
        min_median_threshold = median_body * self.strict_body_ratio

        for idx, m in enumerate(minutes_tuple, start=1):
            c_obj = pair_candles.get(m)
            if not c_obj:
                return False, f"بيانات الشمعة #{idx} مفقودة"

            o_p = float(c_obj["open"])
            c_p = float(c_obj["close"])
            h_p = float(c_obj.get("high", max(o_p, c_p)))
            l_p = float(c_obj.get("low", min(o_p, c_p)))
            b_size = abs(c_p - o_p)
            total_range = max(h_p - l_p, b_size)

            if b_size <= 0:
                return False, f"شمعة #{idx} مسطحة"
            if b_size < (c_p * 0.000015) and b_size < min_median_threshold:
                return False, f"شمعة #{idx} هامشية صغيرة جدًا"
            if total_range > 0 and (b_size / total_range) < 0.14:
                return False, f"شمعة #{idx} دوجي غير واضحة"

        return True, "مطابق ✅"

    def check_indicators(self, asset: str, direction: str, is_pre_close: bool = False) -> tuple[bool, str]:
        if not self.indicators_enabled:
            return True, "متوقف"

        pair_candles = self.candles.get(asset, {})
        sorted_minutes = sorted(pair_candles.keys())
        if len(sorted_minutes) < 6:
            return True, "بيانات أولية"

        ordered_candles = [pair_candles[m] for m in sorted_minutes[-60:]]
        closes = [c["close"] for c in ordered_candles]
        last_c = ordered_candles[-1]
        o_price = last_c["open"]
        c_price = last_c["close"]
        h_price = last_c.get("high", max(o_price, c_price))
        l_price = last_c.get("low", min(o_price, c_price))

        if self.ind_wick_enabled and is_pre_close:
            body = abs(c_price - o_price)
            total_range = max(h_price - l_price, body)
            if total_range <= 0 or (body / total_range) < 0.20:
                return False, "جسم الشمعة صغير"
            if direction == "put" and (h_price - max(o_price, c_price)) > (2.2 * body):
                return False, "ذيل علوي طويل"
            if direction == "call" and (min(o_price, c_price) - l_price) > (2.2 * body):
                return False, "ذيل سفلي طويل"

        if self.ind_bb_enabled:
            bb = calc_bollinger(closes, period=20, mult=2.0)
            if bb is not None:
                lower, mid, upper = bb
                half_band = upper - mid
                if half_band > 0:
                    req_zone = self.ind_bb_zone if is_pre_close else max(0.45, self.ind_bb_zone - 0.25)
                    pos = ((c_price - mid) / half_band) if direction == "put" else ((mid - c_price) / half_band)
                    if pos < req_zone:
                        return False, "خارج نطاق BB"

        if self.ind_rsi_enabled:
            rsi = calc_rsi(closes, period=self.ind_rsi_period)
            if rsi is not None:
                if direction == "put":
                    req_ob = self.ind_rsi_overbought if is_pre_close else (self.ind_rsi_overbought - 6.0)
                    if rsi < req_ob:
                        return False, f"RSI ({rsi:.1f})"
                elif direction == "call":
                    req_os = self.ind_rsi_oversold if is_pre_close else (self.ind_rsi_oversold + 6.0)
                    if rsi > req_os:
                        return False, f"RSI ({rsi:.1f})"

        if self.ind_stoch_enabled:
            stoch = calc_stochastic(ordered_candles, k_period=5, smooth=3)
            if stoch is not None:
                if direction == "put" and stoch < (self.ind_stoch_overbought if is_pre_close else self.ind_stoch_overbought - 7.0):
                    return False, f"Stoch ({stoch:.1f})"
                if direction == "call" and stoch > (self.ind_stoch_oversold if is_pre_close else self.ind_stoch_oversold + 7.0):
                    return False, f"Stoch ({stoch:.1f})"

        if self.ind_atr_enabled and is_pre_close:
            atr = calc_atr(ordered_candles, period=14)
            if atr is not None and atr > 0:
                c_range = max(h_price - l_price, abs(c_price - o_price))
                if c_range < (0.25 * atr) or c_range > (4.0 * atr):
                    return False, "تقلب غير مناسب (ATR)"

        return True, "مستوفي ✅"

    def evaluate_pure_indicators_signal(self, asset: str) -> tuple[str | None, str]:
        pair_candles = self.candles.get(asset, {})
        sorted_minutes = sorted(pair_candles.keys())
        if len(sorted_minutes) < 20:
            return None, ""

        ordered_candles = [pair_candles[m] for m in sorted_minutes[-60:]]
        closes = [c["close"] for c in ordered_candles]
        c_price = closes[-1]

        checks_done = []
        put_ok = True
        call_ok = True

        if self.ind_bb_enabled:
            bb = calc_bollinger(closes, period=20, mult=2.0)
            if bb is None:
                return None, ""
            lower, mid, upper = bb
            half_band = upper - mid
            if half_band <= 0:
                return None, ""
            if ((c_price - mid) / half_band) < self.ind_bb_zone:
                put_ok = False
            if ((mid - c_price) / half_band) < self.ind_bb_zone:
                call_ok = False
            checks_done.append("BB")

        if self.ind_rsi_enabled:
            rsi = calc_rsi(closes, period=self.ind_rsi_period)
            if rsi is None:
                return None, ""
            if rsi < self.ind_rsi_overbought:
                put_ok = False
            if rsi > self.ind_rsi_oversold:
                call_ok = False
            checks_done.append(f"RSI:{rsi:.0f}")

        if self.ind_stoch_enabled:
            stoch = calc_stochastic(ordered_candles, k_period=5, smooth=3)
            if stoch is None:
                return None, ""
            if stoch < self.ind_stoch_overbought:
                put_ok = False
            if stoch > self.ind_stoch_oversold:
                call_ok = False
            checks_done.append(f"Stoch:{stoch:.0f}")

        if not checks_done:
            rsi = calc_rsi(closes, period=14)
            if rsi is not None:
                if rsi >= 72.0:
                    return "put", f"RSI:{rsi:.0f}"
                if rsi <= 28.0:
                    return "call", f"RSI:{rsi:.0f}"
            return None, ""

        if put_ok and not call_ok:
            return "put", " + ".join(checks_done)
        if call_ok and not put_ok:
            return "call", " + ".join(checks_done)
        return None, ""

    def get_active_indicators_summary(self) -> str:
        if not self.indicators_enabled and self.strategy_mode != "INDICATORS":
            return "🔴 متوقف"
        active = []
        if self.ind_bb_enabled:
            active.append(f"BB({int(self.ind_bb_zone * 100)}%)")
        if self.ind_rsi_enabled:
            active.append(f"RSI({int(self.ind_rsi_oversold)}/{int(self.ind_rsi_overbought)})")
        if self.ind_wick_enabled:
            active.append("الذيل")
        if self.ind_stoch_enabled:
            active.append("Stoch")
        if self.ind_atr_enabled:
            active.append("ATR")
        return f"🟢 مفعّل ({' + '.join(active) if active else 'RSI(28/72)'})"

    # ==========================================
    # مدقق Gemini 2.5 Flash المجاني (اختياري عبر GEMINI_API_KEY)
    # ==========================================
    async def verify_with_gemini_free(self, asset: str, direction: str, meta: dict) -> tuple[bool, str]:
        if not GEMINI_API_KEY or not self.http_session:
            return True, "محلي"

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        prompt_txt = (
            f"You are an ultra-fast M1 binary options scalping risk filter. "
            f"Asset: {asset}, Proposed Direction: {direction.upper()} (60s expiry). "
            f"Metrics: Z-Score={meta.get('z_score')}, RSI14={meta.get('rsi_14')}, RSI7={meta.get('rsi_7')}, "
            f"EMA20_Dist_ATR={meta.get('ema_dist_atr')}, WickReject%={meta.get('wick_reject_pct')}, "
            f"TickVelocity={meta.get('velocity')}, Last3Candles={json.dumps(meta.get('last_3_candles'))}. "
            f"Reply ONLY with valid JSON: {{\"approve\": true/false, \"reason\": \"short 4 words\"}}."
        )
        payload = {
            "contents": [{"parts": [{"text": prompt_txt}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 60}
        }
        try:
            async with self.http_session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=1.8)) as resp:
                if resp.status != 200:
                    return True, "تجاوز سريع"
                data = await resp.json()
                raw_txt = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                clean_json = raw_txt.replace("```json", "").replace("```", "").strip()
                parsed = json.loads(clean_json)
                approved = bool(parsed.get("approve", True))
                reason = str(parsed.get("reason", "OK"))[:35]
                self.ai_scorer.gemini_calls_today += 1
                self.ai_scorer.gemini_last_verdict = f"{'✅' if approved else '❌'} {reason}"
                return approved, reason
        except Exception:
            return True, "تجاوز مهلة"

    # ==========================================
    # إدارة الحسابات
    # ==========================================
    def _normalize_account_record(self, acc: dict) -> dict:
        base_frame = acc.get("demo_auth_frame") or acc.get("live_auth_frame") or acc.get("auth_frame") or DEFAULT_AUTH_FRAME
        parsed = parse_auth_frame(base_frame) or {}
        uid = str(acc.get("uid") or parsed.get("uid") or "000000")
        demo_f = acc.get("demo_auth_frame") or build_auth_frame_with_mode(base_frame, True)
        live_f = acc.get("live_auth_frame") or ""
        has_dedicated_live = bool(acc.get("has_dedicated_live", False))
        if live_f and demo_f:
            d_p, l_p = parse_auth_frame(demo_f) or {}, parse_auth_frame(live_f) or {}
            if l_p.get("session") and l_p.get("session") != d_p.get("session"):
                has_dedicated_live = True
        if not live_f:
            live_f = build_auth_frame_with_mode(demo_f, False)
        return {
            "name": acc.get("name", f"حساب {uid}"), "uid": uid,
            "demo_auth_frame": demo_f, "live_auth_frame": live_f,
            "has_dedicated_live": has_dedicated_live, "auth_frame": demo_f
        }

    def _apply_env_default_frames(self):
        if not self.accounts:
            return
        env_demo = parse_auth_frame(DEFAULT_AUTH_FRAME)
        env_live = parse_auth_frame(DEFAULT_LIVE_AUTH_FRAME) if DEFAULT_LIVE_AUTH_FRAME.strip() else None
        target_uid = str((env_live or env_demo or {}).get("uid") or self.accounts[0]["uid"])
        target_idx = next((i for i, a in enumerate(self.accounts) if str(a.get("uid")) == target_uid), 0)
        if env_demo and env_demo.get("session"):
            self.accounts[target_idx]["demo_auth_frame"] = build_auth_frame_with_mode(DEFAULT_AUTH_FRAME, True)
            self.accounts[target_idx]["uid"] = str(env_demo.get("uid", self.accounts[target_idx]["uid"]))
        if env_live and env_live.get("session"):
            self.accounts[target_idx]["live_auth_frame"] = build_auth_frame_with_mode(DEFAULT_LIVE_AUTH_FRAME, False)
            self.accounts[target_idx]["has_dedicated_live"] = True

    def _load_accounts_storage(self):
        loaded = False
        if os.path.exists(ACCOUNTS_FILE):
            try:
                with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and data.get("accounts"):
                    self.accounts = [self._normalize_account_record(a) for a in data["accounts"]]
                    self.active_account_idx = min(int(data.get("active_idx", 0)), len(self.accounts) - 1)
                    self.is_demo_mode = bool(data.get("is_demo_mode", True))
                    loaded = True
            except Exception as e:
                log("ACC_WARN", f"تعذر قراءة ملف الحسابات: {e}")

        if not loaded or not self.accounts:
            parsed = parse_auth_frame(DEFAULT_AUTH_FRAME) or {}
            uid = str(parsed.get("uid", "142271281"))
            self.is_demo_mode = True
            self.accounts = [self._normalize_account_record({
                "name": "الحساب الافتراضي", "uid": uid,
                "demo_auth_frame": build_auth_frame_with_mode(DEFAULT_AUTH_FRAME, True),
                "live_auth_frame": build_auth_frame_with_mode(DEFAULT_LIVE_AUTH_FRAME, False) if DEFAULT_LIVE_AUTH_FRAME.strip() else "",
                "has_dedicated_live": bool(DEFAULT_LIVE_AUTH_FRAME.strip())
            })]
            self.active_account_idx = 0

        self._apply_env_default_frames()
        self._save_accounts_storage()
        self._sync_active_auth_frame()

    def _save_accounts_storage(self):
        try:
            with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
                json.dump({"active_idx": self.active_account_idx, "is_demo_mode": self.is_demo_mode, "accounts": self.accounts}, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log("ACC_WARN", f"تعذر حفظ ملف الحسابات: {e}")

    def _sync_active_auth_frame(self):
        if not self.accounts:
            return
        self.active_account_idx = max(0, min(self.active_account_idx, len(self.accounts) - 1))
        acc = self.accounts[self.active_account_idx]
        self.auth_frame = acc.get("demo_auth_frame") if self.is_demo_mode else acc.get("live_auth_frame")

    def get_active_account(self) -> dict:
        if not self.accounts:
            return {"name": "غير محدد", "uid": "000000", "auth_frame": self.auth_frame, "has_dedicated_live": False}
        return self.accounts[self.active_account_idx]

    def update_active_account_single_mode(self, raw_frame: str, target_is_demo: bool) -> tuple[bool, str]:
        parsed = parse_auth_frame(raw_frame)
        if parsed is None or not parsed.get("session"):
            return False, "صيغة غير صحيحة"
        acc = self.accounts[self.active_account_idx]
        uid = str(parsed.get("uid", acc["uid"]))
        acc["uid"] = uid
        if target_is_demo:
            acc["demo_auth_frame"] = build_auth_frame_with_mode(raw_frame, True)
            acc["auth_frame"] = acc["demo_auth_frame"]
        else:
            acc["live_auth_frame"] = build_auth_frame_with_mode(raw_frame, False)
            acc["has_dedicated_live"] = True
        self.account_balance = None
        self.limit_start_balance = None
        self.reset_boost_state(reset_peak=True)
        self._sync_active_auth_frame()
        self._save_accounts_storage()
        return True, uid

    # ==========================================
    # التعزيز والإحصائيات والمسح
    # ==========================================
    def reset_boost_state(self, reset_peak: bool = False):
        self.in_boost_recovery = False
        self.boost_wins_done = 0
        self.current_cycle_spent = 0.0
        self.effective_base_amount = self.base_amount
        if reset_peak:
            self.peak_balance = float(self.account_balance) if self.account_balance is not None else None

    def reset_daily_stats(self):
        self.stats_date = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
        self.daily_records.clear()
        self.total_turnover = 0.0
        self._save_bot_state()

    def record_trade_outcome(self, pair_name: str, is_win: bool, attempt_num: int):
        today_str = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
        if today_str != self.stats_date:
            self.reset_daily_stats()
        self.daily_records.append({"pair": pair_name, "result": "WIN" if is_win else "LOSS", "step": attempt_num})
        self._save_bot_state()

    def format_daily_stats_report(self) -> str:
        now_dt = datetime.now(tz=TZ_UTC3)
        days_ar = {"Monday": "الإثنين", "Tuesday": "الثلاثاء", "Wednesday": "الأربعاء", "Thursday": "الخميس", "Friday": "الجمعة", "Saturday": "السبت", "Sunday": "الأحد"}
        date_header = f"{now_dt.strftime('%d %b %Y')} • {days_ar.get(now_dt.strftime('%A'), now_dt.strftime('%A'))}"
        total_signals = len(self.daily_records)
        wins = [r for r in self.daily_records if r["result"] == "WIN"]
        losses = [r for r in self.daily_records if r["result"] == "LOSS"]
        win_count, loss_count = len(wins), len(losses)
        win_rate = (win_count / total_signals * 100.0) if total_signals > 0 else 0.0

        sure_shot = sum(1 for r in wins if r["step"] == 1)
        mtg1 = sum(1 for r in wins if r["step"] == 2)
        mtg2 = sum(1 for r in wins if r["step"] == 3)
        mtg3_plus = sum(1 for r in wins if r["step"] >= 4)
        pct = lambda v: round((v / total_signals) * 100) if total_signals > 0 else 0

        best_streak = curr_streak = 0
        for r in self.daily_records:
            if r["result"] == "WIN":
                curr_streak += 1
                best_streak = max(best_streak, curr_streak)
            else:
                curr_streak = 0

        step_badges = {1: "✅", 2: "1️⃣", 3: "2️⃣", 4: "3️⃣", 5: "4️⃣", 6: "5️⃣"}
        icons = [step_badges.get(r["step"], "✅") if r["result"] == "WIN" else "❌" for r in self.daily_records]
        timeline_block = "\n".join(f"┃ {' '.join(icons[i:i + 10])}" for i in range(0, len(icons), 10)) if icons else "┃ <i>لا توجد صفقات مسجلة اليوم بعد</i>"

        pair_stats = {}
        for r in self.daily_records:
            p = r["pair"]
            pair_stats.setdefault(p, {"wins": 0, "total": 0})
            pair_stats[p]["total"] += 1
            if r["result"] == "WIN":
                pair_stats[p]["wins"] += 1
        top_pairs = sorted(pair_stats.items(), key=lambda x: (x[1]["wins"], x[1]["wins"] / x[1]["total"]), reverse=True)[:3]
        top_pairs_str = "  |  ".join(f"<code>{p}</code> ({d['wins']}/{d['total']})" for p, d in top_pairs) if top_pairs else "لا يوجد بعد"

        mtg3_line = f"┃ 🔵 <b>MTG 3+</b>    {make_progress_bar(mtg3_plus, total_signals)}  <b>{mtg3_plus}</b> ({pct(mtg3_plus)}%)\n" if mtg3_plus > 0 else ""
        return (
            f"╭━━━[ 🛡 <b>عبود تريدينج</b> ]━━━╮\n"
            f"┃ 📊 <b>DAILY RESULTS — النتائج اليومية</b>\n"
            f"┃ 📅 <code>{date_header}</code>\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"┃ 🎯 <b>WIN RATE:</b> <b>{win_rate:.1f}%</b>  |  📡 <b>SIGNALS:</b> <b>{total_signals}</b>\n"
            f"┃ 🟢 <b>{win_count} WINS</b>      🔴 <b>{loss_count} LOSSES</b>\n"
            f"┃ 💵 <b>عائد التداول:</b> <b>${format_money(self.total_turnover)}</b>\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"┃ 🔥 <b>BEST STREAK: {best_streak} WINS</b>\n"
            f"┃ 🟢 <b>SURE SHOT</b>  {make_progress_bar(sure_shot, total_signals)}  <b>{sure_shot}</b> ({pct(sure_shot)}%)\n"
            f"┃ 🟡 <b>MTG 1</b>         {make_progress_bar(mtg1, total_signals)}  <b>{mtg1}</b> ({pct(mtg1)}%)\n"
            f"┃ 🟠 <b>MTG 2</b>         {make_progress_bar(mtg2, total_signals)}  <b>{mtg2}</b> ({pct(mtg2)}%)\n"
            f"{mtg3_line}"
            f"┃ 🔴 <b>LOSS</b>           {make_progress_bar(loss_count, total_signals)}  <b>{loss_count}</b> ({pct(loss_count)}%)\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"{timeline_block}\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"┃ 🏆 <b>TOP PAIRS:</b> {top_pairs_str}\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━╯"
        )

    def get_scope_label(self) -> str:
        return {
            "CURRENCIES": "💱 العملات فقط", "CRYPTO": "🪙 الكريبتو فقط",
            "STOCKS": "📈 الأسهم فقط", "CURR_CRYPTO": "💱+🪙 العملات والكريبتو",
            "ALL": "🌐 الكل (عملات + كريبتو + أسهم)",
        }.get(self.scan_scope, self.scan_scope)

    def get_active_scan_pairs(self) -> list[str]:
        if self.scan_scope == "CURRENCIES":
            pairs = self.assets_by_cat["CURRENCIES"]
        elif self.scan_scope == "CRYPTO":
            pairs = self.assets_by_cat["CRYPTO"]
        elif self.scan_scope == "STOCKS":
            pairs = self.assets_by_cat["STOCKS"]
        elif self.scan_scope == "CURR_CRYPTO":
            pairs = self.assets_by_cat["CURRENCIES"] + self.assets_by_cat["CRYPTO"]
        else:
            pairs = self.assets_by_cat["CURRENCIES"] + self.assets_by_cat["CRYPTO"] + self.assets_by_cat["STOCKS"]
        return list(dict.fromkeys(pairs))

    def format_signal_symbol(self, symbol: str) -> str:
        if symbol in self.asset_display_names and self.asset_display_names[symbol]:
            return self.asset_display_names[symbol].replace("/", "").replace(" OTC", "-OTC").strip()
        return symbol.replace("_otc", "-OTC").replace("#", "")

    def get_current_entry_base(self) -> float:
        return self.effective_base_amount if (self.boost_enabled and self.in_boost_recovery) else self.base_amount

    def get_trade_ladder(self, custom_base: float | None = None) -> list[float]:
        active_base = custom_base if custom_base is not None else self.get_current_entry_base()
        steps = self.global_steps if self.martingale_enabled else 1
        return [int(active_base * (2 ** i)) if (active_base * (2 ** i)).is_integer() else round(active_base * (2 ** i), 2) for i in range(steps)]

    def get_ws_url(self) -> str:
        return "wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket" if self.is_demo_mode else "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket"

    def get_server_now(self) -> float:
        return time.time() + self.server_hour_shift

    def refresh_mode_state(self):
        self.mode = "IN_TRADE" if self.active_trades else ("LOCKED" if self.locked_candidates else "SCANNING")

    def enqueue_new_signal(self, text: str) -> int:
        self.signal_seq += 1
        self.channel_queue.put_nowait(("NEW", self.signal_seq, text))
        return self.signal_seq

    def enqueue_signal_reply(self, signal_uid: int | None, text: str):
        self.channel_queue.put_nowait(("REPLY", signal_uid, text))

    async def ensure_subscribed(self, pair: str):
        if self.ws and self.authenticated and pair not in self.subscribed_pairs:
            try:
                await self.ws.send(f'42["subfor","{pair}"]')
                self.subscribed_pairs.add(pair)
            except Exception:
                pass

    async def subscribe_all_eligible_pairs(self):
        if not self.ws or not self.authenticated:
            return
        for pair in self.get_active_scan_pairs():
            eligible, _ = self.is_pair_eligible(pair)
            if eligible and pair not in self.subscribed_pairs:
                await self.ensure_subscribed(pair)
                await asyncio.sleep(0.02)

    def switch_pair_keep_global(self, reason: str):
        log("MODE", f"تغيير الزوج مع استمرار التدبيل العام ({self.global_step_index + 1}/{self.global_steps}) — {reason}")
        self.mode = "SCANNING"
        self.locked_candidates.clear()
        self.active_trades.clear()

    def full_reset_to_scanning(self, reason: str):
        log("MODE", f"إعادة ضبط كاملة والعودة للمسح ({reason})")
        self.mode = "SCANNING"
        self.locked_candidates.clear()
        self.active_trades.clear()
        self.global_step_index = 0
        self.pending_martingale_steps.clear()
        self.ai_scorer.reset()
        self.ind_strategy_cooldown.clear()

    # ==========================================
    # واجهات وأزرار لوحة التحكم
    # ==========================================
    def build_main_keyboard(self) -> dict:
        toggle_btn = {"text": "⏹ إيقاف التداول", "callback_data": "cmd_stop"} if self.is_running else {"text": "▶️ بدء التداول", "callback_data": "cmd_start"}
        strat_labels = {
            "CHESS": "♟ الاستراتيجية: الشطرنج (اضغط للتبديل) 🔄",
            "INDICATORS": "📊 الاستراتيجية: المؤشرات المستقلة 🔄",
            "AI": "🧠 الاستراتيجية: الذكاء الهجين AI (Ensemble+Gemini) 🔄"
        }
        strategy_btn = {"text": strat_labels[self.strategy_mode], "callback_data": "cycle_strategy_mode"}
        mode_btn = {"text": "🧪 الحساب: تجريبي (Demo)", "callback_data": "toggle_account_mode"} if self.is_demo_mode else {"text": "💵 الحساب: حقيقي (Live) 🔥", "callback_data": "toggle_account_mode"}
        limits_btn = {"text": "🎯 الحدود: مفعّل 🟢", "callback_data": "toggle_limits_master"} if self.limits_enabled else {"text": "🎯 الحدود: متوقف 🔴", "callback_data": "toggle_limits_master"}
        strict_chess_btn = {"text": "⚔️ الشطرنج الشديد: مفعّل 🟢", "callback_data": "toggle_strict_chess"} if self.strict_chess_enabled else {"text": "⚔️ الشطرنج الشديد: متوقف 🔴", "callback_data": "toggle_strict_chess"}
        sixth_btn = {"text": "🕯️ الشمعة السادسة: مفعّل 🟢", "callback_data": "toggle_sixth_candle"} if self.sixth_candle_enabled else {"text": "🕯️ الشمعة السادسة: متوقف 🔴", "callback_data": "toggle_sixth_candle"}
        ind_master_btn = {"text": "📊 فلتر المؤشرات: مفعّل 🟢", "callback_data": "toggle_indicators_master"} if self.indicators_enabled else {"text": "📊 فلتر المؤشرات: متوقف 🔴", "callback_data": "toggle_indicators_master"}
        fast_btn = {"text": "⚡ الوضع السريع: مفعّل 🟢", "callback_data": "toggle_fast_mode"} if self.fast_mode else {"text": "⚡ الوضع السريع: متوقف 🔴", "callback_data": "toggle_fast_mode"}
        mtg_toggle_btn = {"text": "🔁 التدبيل: مفعّل 🟢", "callback_data": "toggle_martingale"} if self.martingale_enabled else {"text": "🔁 التدبيل: متوقف 🔴", "callback_data": "toggle_martingale"}
        boost_btn = {"text": "🛡️ التعزيز: مفعّل 🟢", "callback_data": "toggle_boost"} if self.boost_enabled else {"text": "🛡️ التعزيز: متوقف 🔴", "callback_data": "toggle_boost"}
        cont_btn = {"text": "🔄 مواصلة النموذج: مفعّل 🟢", "callback_data": "toggle_cont"} if self.continuation_enabled else {"text": "🔄 مواصلة النموذج: متوقف 🔴", "callback_data": "toggle_cont"}

        dual_label = f"👯 المزدوج ({self.max_locked_pairs} عملات)" if self.max_locked_pairs > 1 else "👯 المزدوج (1 عملة)"
        trades_label = f"🎯 الصفقات ({self.max_concurrent_trades} من {self.max_locked_pairs})"

        rows = [
            [toggle_btn, {"text": "📡 تحديث الحالة", "callback_data": "cmd_status"}],
            [strategy_btn],
            [mode_btn, {"text": "👤 إدارة الحسابات", "callback_data": "menu_accounts"}],
            [limits_btn, {"text": "⚙️ ضبط الحدود", "callback_data": "menu_limits"}],
        ]
        if self.strategy_mode == "CHESS":
            rows.append([strict_chess_btn, sixth_btn])
        elif self.strategy_mode == "AI":
            rows.append([
                {"text": f"🎚 ثقة AI ({int(self.ai_min_confidence * 100)}%)", "callback_data": "set_ai_conf"},
                {"text": "🧠 مركز الذكاء والتصدير", "callback_data": "menu_ai_train"}
            ])

        rows.extend([
            [{"text": f"🔭 المسح: {self.get_scope_label()}", "callback_data": "menu_scope"}],
            [ind_master_btn, {"text": "⚙️ ضبط المؤشرات", "callback_data": "menu_indicators"}],
            [{"text": dual_label, "callback_data": "set_dual_pairs"}, {"text": trades_label, "callback_data": "set_concurrent_trades"}],
            [fast_btn, cont_btn],
            [{"text": f"💵 الأساسي (${self.base_amount:g})", "callback_data": "set_amount"}, {"text": f"📈 العائد (≥ {self.min_payout}%)", "callback_data": "set_payout"}],
            [mtg_toggle_btn, {"text": f"🌐 التدبيل العام ({self.global_steps})", "callback_data": "set_global_steps"}],
            [{"text": f"🏛 تدبيل الزوج ({self.pair_steps})", "callback_data": "set_pair_steps"}, {"text": f"⏱ التقديم ({self.entry_lead_seconds}ث)", "callback_data": "set_lead"}],
            [boost_btn, {"text": "⚙️ ضبط التعزيز", "callback_data": "menu_boost"}],
            [{"text": "📊 معاينة الإحصائيات", "callback_data": "stats_preview"}, {"text": "📢 نشر بالقناة", "callback_data": "stats_publish"}],
            [{"text": "🗑️ تصفير الإحصائيات", "callback_data": "stats_reset"}]
        ])
        return {"inline_keyboard": rows}

    def format_ai_menu_text(self) -> str:
        model_st = "🟢 <b>مُدرَّب ونشط (Ensemble: LightGBM + RF + ET)</b>" if self.ai_scorer.model is not None else "🟡 <b>طور جمع البيانات (يعمل بحارس الاتجاه AI Guard)</b>"
        gem_st = f"🟢 <b>مفعّل ({GEMINI_MODEL})</b> | فحوصات اليوم: <code>{self.ai_scorer.gemini_calls_today}</code>" if GEMINI_API_KEY else "⚪ <b>اختياري</b> (أضف <code>GEMINI_API_KEY</code> في Render لتفعيل التدقيق المزدوج مجانًا)"
        return (
            f"🧠 <b>مركز الذكاء الاصطناعي الهجين (Ensemble + Gemini)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>النموذج المحلي:</b> {model_st}\n"
            f"✨ <b>مدقق Gemini المجاني:</b> {gem_st}\n"
            f"📊 <b>العينات المجمعة (10 ميزات):</b> <code>{self.ai_scorer.samples_count} عينة</code>\n"
            f"🎚 <b>عتبة الثقة المطلوبة للدخول:</b> <code>{int(self.ai_min_confidence * 100)}%</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>يجمع البوت عينات السوق تلقائيًا (مع حارس الاتجاه EMA20/ATR لمنع عكس الترند)؛ وعند بلوغ 80 عينة فأكثر اضغط «تدريب النموذج» ثم اسحب الملفات لحفظها.</i>"
        )

    def build_ai_menu_keyboard(self) -> dict:
        return {
            "inline_keyboard": [
                [{"text": "🚀 تدريب نموذج Ensemble الثلاثي الآن (مجانًا)", "callback_data": "ai_run_train"}],
                [{"text": "📥 سحب ملفات AI (العينات + النموذج) للتحميل", "callback_data": "ai_export_files"}],
                [{"text": f"🎚 ضبط نسبة الثقة ({int(self.ai_min_confidence * 100)}%)", "callback_data": "set_ai_conf"}],
                [{"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}]
            ]
        }

    def build_limits_keyboard(self) -> dict:
        en_btn = {"text": "🎯 حالة الحدود: مفعّل 🟢", "callback_data": "limits_toggle_enable"} if self.limits_enabled else {"text": "🎯 حالة الحدود: متوقف 🔴", "callback_data": "limits_toggle_enable"}
        return {
            "inline_keyboard": [
                [en_btn],
                [{"text": f"📈 حد الربح (+${self.profit_limit:g})", "callback_data": "set_profit_limit"}, {"text": f"📉 حد الخسارة (-${self.loss_limit:g})", "callback_data": "set_loss_limit"}],
                [{"text": "🔄 تصفير نقطة البداية من الرصيد الحالي", "callback_data": "limits_reset_base"}],
                [{"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}]
            ]
        }

    def format_limits_menu_text(self) -> str:
        en_str = "🟢 <b>مفعّل</b>" if self.limits_enabled else "🔴 <b>متوقف</b>"
        bal_str = f"${format_money(self.account_balance)}" if self.account_balance is not None else "غير متاح"
        if self.limit_start_balance is not None:
            start_str = f"${format_money(self.limit_start_balance)}"
            p_str = f"${format_money(self.limit_start_balance + self.profit_limit)}" if self.profit_limit > 0 else "—"
            l_str = f"${format_money(max(0.0, self.limit_start_balance - self.loss_limit))}" if self.loss_limit > 0 else "—"
        else:
            start_str, p_str, l_str = "عند التشغيل", f"+${self.profit_limit:g}", f"-${self.loss_limit:g}"
        return (
            f"🎯 <b>إعدادات حدود الربح والخسارة</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>الحالة:</b> {en_str} | 💰 <b>الرصيد:</b> <code>{bal_str}</code>\n"
            f"📍 <b>نقطة البداية:</b> <code>{start_str}</code>\n"
            f"📈 <b>حد الربح:</b> <code>+${self.profit_limit:g}</code> (عند <b>{p_str}</b>)\n"
            f"📉 <b>حد الخسارة:</b> <code>-${self.loss_limit:g}</code> (عند <b>{l_str}</b>)"
        )

    def build_boost_keyboard(self) -> dict:
        en_btn = {"text": "🛡️ حالة التعزيز: مفعّل 🟢", "callback_data": "boost_toggle_enable"} if self.boost_enabled else {"text": "🛡️ حالة التعزيز: متوقف 🔴", "callback_data": "boost_toggle_enable"}
        mode_btn = {"text": "🎯 نوع التعزيز: العودة لقمة الرصيد 💰", "callback_data": "boost_toggle_mode"} if self.boost_mode == "CAPITAL" else {"text": "🎯 نوع التعزيز: عدد صفقات ناجحة 🏆", "callback_data": "boost_toggle_mode"}
        return {
            "inline_keyboard": [
                [en_btn], [mode_btn],
                [{"text": f"➕ زيادة التعزيز (+${self.boost_increment:g})", "callback_data": "set_boost_inc"}, {"text": f"🔢 عدد الصفقات ({self.boost_target_wins})", "callback_data": "set_boost_wins"}],
                [{"text": "🔄 تصفير التعزيز والعودة للأساسي", "callback_data": "boost_reset_now"}],
                [{"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}]
            ]
        }

    def format_boost_menu_text(self) -> str:
        en_str = "🟢 <b>مفعّل</b>" if self.boost_enabled else "🔴 <b>متوقف</b>"
        mode_str = "💰 <b>العودة لقمة الرصيد</b>" if self.boost_mode == "CAPITAL" else f"🏆 <b>{self.boost_target_wins} صفقات ناجحة</b>"
        peak_str = f"${format_money(self.peak_balance)}" if self.peak_balance is not None else "غير مسجل"
        return (
            f"🛡️ <b>إعدادات التعزيز الذكي</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>الحالة:</b> {en_str} | 🎯 <b>النظام:</b> {mode_str}\n"
            f"➕ <b>الزيادة:</b> <code>+${self.boost_increment:g}</code> | 🏔 <b>القمة:</b> <code>{peak_str}</code>"
        )

    def build_indicators_keyboard(self) -> dict:
        return {
            "inline_keyboard": [
                [{"text": f"🎛 التفعيل العام كفلتر: {'مفعّل 🟢' if self.indicators_enabled else 'متوقف 🔴'}", "callback_data": "ind_toggle_master"}],
                [{"text": f"📈 البولينجر ({int(self.ind_bb_zone * 100)}%): {'🟢' if self.ind_bb_enabled else '🔴'}", "callback_data": "ind_toggle_bb"}, {"text": "🎚 حساسية BB", "callback_data": "ind_set_bb"}],
                [{"text": f"📉 RSI ({int(self.ind_rsi_oversold)}/{int(self.ind_rsi_overbought)}): {'🟢' if self.ind_rsi_enabled else '🔴'}", "callback_data": "ind_toggle_rsi"}, {"text": "🎚 مستويات RSI", "callback_data": "ind_set_rsi"}],
                [{"text": f"🕯️ فلتر الذيل: {'🟢' if self.ind_wick_enabled else '🔴'}", "callback_data": "ind_toggle_wick"}, {"text": f"⚡ ستوكاستيك: {'🟢' if self.ind_stoch_enabled else '🔴'}", "callback_data": "ind_toggle_stoch"}],
                [{"text": f"🌊 فلتر السيولة ATR: {'🟢' if self.ind_atr_enabled else '🔴'}", "callback_data": "ind_toggle_atr"}],
                [{"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}]
            ]
        }

    def format_indicators_menu_text(self) -> str:
        return (
            f"📊 <b>إعدادات المؤشرات والفلاتر</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>البولينجر (20, 2):</b> {'🟢' if self.ind_bb_enabled else '🔴'} (<code>{int(self.ind_bb_zone * 100)}%</code>)\n"
            f"• <b>مؤشر RSI (14):</b> {'🟢' if self.ind_rsi_enabled else '🔴'} (<code>{int(self.ind_rsi_oversold)} / {int(self.ind_rsi_overbought)}</code>)\n"
            f"• <b>فلتر الذيل:</b> {'🟢' if self.ind_wick_enabled else '🔴'} | <b>ستوكاستيك:</b> {'🟢' if self.ind_stoch_enabled else '🔴'} | <b>ATR:</b> {'🟢' if self.ind_atr_enabled else '🔴'}"
        )

    def build_accounts_keyboard(self) -> dict:
        rows = []
        for idx, acc in enumerate(self.accounts):
            prefix = "✅ " if idx == self.active_account_idx else "👤 "
            live_tag = "Live✅" if acc.get("has_dedicated_live") else "Live⚠️"
            rows.append([{"text": f"{prefix}{acc['name']} ({acc['uid']}) [Demo✅|{live_tag}]", "callback_data": f"select_acc_{idx}"}])
        rows.append([{"text": "🔑 تحديث جلسة الديمو", "callback_data": "set_demo_auth"}, {"text": "🔥 تحديث جلسة الحقيقي", "callback_data": "set_live_auth"}])
        rows.append([{"text": "⌨️ إضافة حساب يدوي", "callback_data": "add_account_start"}, {"text": "🗑️ حذف حساب", "callback_data": "menu_delete_account"}])
        rows.append([{"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}])
        return {"inline_keyboard": rows}

    def build_delete_accounts_keyboard(self) -> dict:
        rows = [[{"text": f"🗑️ حذف: {acc['name']} ({acc['uid']})", "callback_data": f"del_acc_{idx}"}] for idx, acc in enumerate(self.accounts)]
        rows.append([{"text": "🔙 رجوع لإدارة الحسابات", "callback_data": "menu_accounts"}])
        return {"inline_keyboard": rows}

    def build_scope_keyboard(self) -> dict:
        mark = lambda c, t: f"✅ {t}" if self.scan_scope == c else t
        return {
            "inline_keyboard": [
                [{"text": mark("CURRENCIES", "💱 أزواج العملات فقط"), "callback_data": "scope_CURRENCIES"}],
                [{"text": mark("CRYPTO", "🪙 أزواج الكريبتو فقط"), "callback_data": "scope_CRYPTO"}],
                [{"text": mark("STOCKS", "📈 الأسهم فقط"), "callback_data": "scope_STOCKS"}],
                [{"text": mark("CURR_CRYPTO", "💱 + 🪙 العملات والكريبتو معًا"), "callback_data": "scope_CURR_CRYPTO"}],
                [{"text": mark("ALL", "🌐 جميع الأسواق"), "callback_data": "scope_ALL"}],
                [{"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}]
            ]
        }

    def format_dashboard_text(self) -> str:
        ladder = self.get_trade_ladder()
        ladder_str = " ⬅️ ".join(f"${x}" for x in ladder)
        current_next_amt = ladder[min(self.global_step_index, len(ladder) - 1)]
        status_icon = "🟢 يعمل الآن" if self.is_running else "🔴 متوقف"
        conn_icon = "متصل مستقر ✅" if self.authenticated else "جاري الاتصال ⏳"

        acc = self.get_active_account()
        acc_mode_str = "🧪 تجريبي (Demo)" if self.is_demo_mode else "💵 حقيقي (Live) 🔥"
        bal_str = f"${format_money(self.account_balance)}" if self.account_balance is not None else "جاري الجلب..."

        active_pairs = self.get_active_scan_pairs()
        eligible_count = sum(1 for p in active_pairs if self.payouts.get(p, 0) >= self.min_payout)
        total_today = len(self.daily_records)
        wins_today = sum(1 for r in self.daily_records if r["result"] == "WIN")
        rate_today = (wins_today / total_today * 100.0) if total_today > 0 else 0.0

        if self.strategy_mode == "AI":
            ens_tag = "Ensemble مُدرَّب ✅" if self.ai_scorer.model is not None else f"جامع بيانات ({self.ai_scorer.samples_count} عينة)"
            gem_tag = " + Gemini✅" if GEMINI_API_KEY else ""
            strat_line = f"🧠 <b>الاستراتيجية:</b> <b>الذكاء الهجين AI</b> ({ens_tag}{gem_tag} | ثقة ≥ <code>{int(self.ai_min_confidence * 100)}%</code>)"
            sub_strat_line = f"📡 <b>الأزواج المراقبة لحظيًا:</b> <code>{len(self.subscribed_pairs)}</code> عملة متصلة"
        elif self.strategy_mode == "INDICATORS":
            strat_line = "📊 <b>الاستراتيجية:</b> <b>المؤشرات المستقلة (دخول فوري بدون شطرنج)</b>"
            sub_strat_line = f"📡 <b>الأزواج المراقبة لحظيًا:</b> <code>{len(self.subscribed_pairs)}</code> عملة متصلة"
        else:
            strat_line = "♟ <b>الاستراتيجية:</b> <b>الشطرنج الاعتيادي (إغلاق الشموع)</b>"
            sub_strat_line = f"⚔️ <b>الشطرنج الشديد:</b> {'🟢 مفعّل' if self.strict_chess_enabled else '🔴 متوقف'} | 🕯️ <b>الشمعة السادسة:</b> {'🟢 6 شموع' if self.sixth_candle_enabled else '🔴 5 شموع'}"

        limits_status = f"🟢 مفعّل (+${self.profit_limit:g} / -${self.loss_limit:g})" if self.limits_enabled else "🔴 متوقف"
        mtg_status = f"🟢 مفعّل (<code>{ladder_str}</code>)" if self.martingale_enabled else f"🔴 متوقف (<code>${current_next_amt}</code>)"
        boost_status = f"🟢 مفعّل (+${self.boost_increment:g})" if self.boost_enabled else "🔴 متوقف"

        activity_parts = []
        if self.active_trades:
            activity_parts.append(f"⚡ نشط ({len(self.active_trades)}): " + " | ".join(f"{self.format_signal_symbol(t['asset'])} (${t['amount']})" for t in self.active_trades))
        if self.locked_candidates:
            activity_parts.append(f"🎯 مجهّز ({len(self.locked_candidates)}/{self.max_locked_pairs})")
        if not activity_parts:
            activity_parts.append(f"🔍 يبحث عن فرصة (الدخول القادم: ${current_next_amt})")

        return (
            f"♟ <b>لوحة إدارة عبود تريدينج (24/7)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>الحالة:</b> {status_icon}\n"
            f"{strat_line}\n"
            f"{sub_strat_line}\n"
            f"👤 <b>الحساب:</b> <b>{html.escape(acc['name'])}</b> (<code>{acc['uid']}</code>) | {acc_mode_str}\n"
            f"💰 <b>الرصيد:</b> <code>{bal_str}</code> | 🌐 <b>السيرفر:</b> {conn_icon}\n"
            f"📍 <b>النشاط:</b> {' + '.join(activity_parts)}\n"
            f"📊 <b>اليوم:</b> <code>{wins_today}/{total_today}</code> ({rate_today:.1f}%) | 💵 <b>العائد:</b> <code>${format_money(self.total_turnover)}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>الحدود:</b> {limits_status} | 🛡️ <b>التعزيز:</b> {boost_status}\n"
            f"🔭 <b>المسح:</b> <b>{self.get_scope_label()}</b> ({eligible_count}/{len(active_pairs)})\n"
            f"📊 <b>المؤشرات:</b> {self.get_active_indicators_summary()}\n"
            f"👯 <b>المزدوج / الصفقات:</b> <code>{self.max_locked_pairs}</code> عملات | <code>{self.max_concurrent_trades}</code> صفقات\n"
            f"⚡ <b>الوضع السريع:</b> {'🟢 مفعّل' if self.fast_mode else '🔴 متوقف'} | 🔄 <b>مواصلة النموذج:</b> {'🟢 مفعّل' if self.continuation_enabled else '🔴 متوقف'}\n"
            f"💵 <b>الأساسي:</b> <code>${self.base_amount:g}</code> | 🔁 <b>التدبيل:</b> {mtg_status}\n"
            f"🏛 <b>تدبيل الزوج:</b> <code>{self.pair_steps}</code> | ⏱ <b>التقديم:</b> <code>{self.entry_lead_seconds}ث</code>"
        )

    # ==========================================
    # الاتصال بتليجرام والقناة وإرسال الملفات
    # ==========================================
    async def tg_api(self, method: str, payload: dict, req_timeout: float = 15.0, retries: int = 3):
        if not self.http_session:
            return None
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
        for attempt in range(1, retries + 1):
            try:
                async with self.http_session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=req_timeout)) as resp:
                    return await resp.json()
            except asyncio.CancelledError:
                raise
            except Exception:
                if attempt == retries:
                    return None
                await asyncio.sleep(0.5 * attempt)
        return None

    async def send_admin(self, text: str, reply_markup: dict = None):
        payload = {"chat_id": ADMIN_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        await self.tg_api("sendMessage", payload)

    async def send_admin_document(self, file_path: str, caption: str = "") -> bool:
        if not self.http_session or not os.path.exists(file_path):
            return False
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        form = aiohttp.FormData()
        form.add_field("chat_id", str(ADMIN_CHAT_ID))
        if caption:
            form.add_field("caption", caption)
            form.add_field("parse_mode", "HTML")
        with open(file_path, "rb") as f:
            form.add_field("document", f.read(), filename=os.path.basename(file_path))
        try:
            async with self.http_session.post(url, data=form, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                res = await resp.json()
                return bool(res and res.get("ok"))
        except Exception as e:
            log("TG_DOC_ERR", f"فشل إرسال الملف {file_path}: {e}")
            return False

    async def send_channel(self, text: str, reply_to_message_id: int = None) -> int | None:
        if not CHANNEL_ID or "ضع_" in str(CHANNEL_ID):
            return None
        payload = {"chat_id": CHANNEL_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
            payload["allow_sending_without_reply"] = True
        res = await self.tg_api("sendMessage", payload)
        return res.get("result", {}).get("message_id") if (res and res.get("ok")) else None

    async def channel_sender_worker(self):
        while True:
            kind, uid, text = await self.channel_queue.get()
            try:
                if kind == "NEW":
                    msg_id = await self.send_channel(text)
                    if uid is not None and msg_id is not None:
                        self.signal_msg_ids[uid] = msg_id
                        if len(self.signal_msg_ids) > 150:
                            del self.signal_msg_ids[min(self.signal_msg_ids.keys())]
                elif kind == "REPLY":
                    await self.send_channel(text, reply_to_message_id=self.signal_msg_ids.get(uid) if uid else None)
            finally:
                self.channel_queue.task_done()

    async def daily_midnight_stats_scheduler(self):
        while True:
            await asyncio.sleep(20)
            today_str = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
            if today_str != self.stats_date:
                if self.daily_records or self.total_turnover > 0:
                    await self.send_channel(self.format_daily_stats_report())
                self.reset_daily_stats()

    # ==========================================
    # استقبال أوامر وأزرار تليجرام
    # ==========================================
    async def telegram_polling_loop(self):
        offset = 0
        while True:
            try:
                res = await self.tg_api("getUpdates", {"offset": offset, "timeout": 15}, req_timeout=25.0, retries=1)
                if not res or not res.get("ok"):
                    await asyncio.sleep(1.5)
                    continue

                for update in res.get("result", []):
                    offset = update["update_id"] + 1
                    if "callback_query" in update:
                        cq = update["callback_query"]
                        if str(cq["message"]["chat"]["id"]) != str(ADMIN_CHAT_ID):
                            continue
                        msg_id = cq["message"]["message_id"]
                        data = cq.get("data", "")

                        if data == "stats_reset":
                            self.reset_daily_stats()
                            asyncio.create_task(self.tg_api("answerCallbackQuery", {"callback_query_id": cq["id"], "text": "✅ تم تصفير الإحصائيات!", "show_alert": True}, retries=1))
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_dashboard_text(), "parse_mode": "HTML", "reply_markup": self.build_main_keyboard()})
                            continue

                        if data == "ai_run_train":
                            _, train_msg = self.ai_scorer.train_model_now()
                            asyncio.create_task(self.tg_api("answerCallbackQuery", {"callback_query_id": cq["id"], "text": train_msg[:195], "show_alert": True}, retries=1))
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_ai_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_ai_menu_keyboard()})
                            continue

                        if data == "ai_export_files":
                            asyncio.create_task(self.tg_api("answerCallbackQuery", {"callback_query_id": cq["id"], "text": "⏳ جاري سحب وإرسال ملفات AI..."}, retries=1))
                            sent_any = False
                            if os.path.exists(AI_DATASET_FILE):
                                ok1 = await self.send_admin_document(
                                    AI_DATASET_FILE,
                                    f"📊 <b>ملف عينات التدريب</b> ({self.ai_scorer.samples_count} عينة)"
                                )
                                sent_any = sent_any or ok1
                            if os.path.exists(AI_MODEL_FILE):
                                ok2 = await self.send_admin_document(
                                    AI_MODEL_FILE,
                                    "🧠 <b>نموذج Ensemble الثلاثي المُدرَّب</b> (ارفعه لمستودع GitHub ليحمله البوت تلقائيًا)"
                                )
                                sent_any = sent_any or ok2
                            if not sent_any:
                                await self.send_admin("⚠️ لا توجد ملفات بيانات أو نموذج محفوظة حتى الآن.")
                            continue

                        asyncio.create_task(self.tg_api("answerCallbackQuery", {"callback_query_id": cq["id"]}, retries=1))

                        if data == "cycle_strategy_mode":
                            modes = ["CHESS", "INDICATORS", "AI"]
                            self.strategy_mode = modes[(modes.index(self.strategy_mode) + 1) % len(modes)]
                            self._save_bot_state()
                            self.switch_pair_keep_global(f"التبديل إلى {self.strategy_mode}")
                            if self.strategy_mode in ("AI", "INDICATORS"):
                                asyncio.create_task(self.subscribe_all_eligible_pairs())
                        elif data == "menu_ai_train":
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_ai_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_ai_menu_keyboard()})
                            continue
                        elif data == "set_ai_conf":
                            self.awaiting_input = "AI_CONFIDENCE"
                            await self.send_admin("🧠 أرسل نسبة ثقة الذكاء الاصطناعي المطلوبة (من <code>65</code> إلى <code>95</code>؛ الموصى به <code>82</code>):")
                            continue
                        elif data == "cmd_start":
                            self.is_running = True
                            self.awaiting_input = None
                            if self.limits_enabled:
                                self.capture_start_balance_on_run()
                            if self.strategy_mode in ("AI", "INDICATORS"):
                                asyncio.create_task(self.subscribe_all_eligible_pairs())
                        elif data == "cmd_stop":
                            self.is_running = False
                            self.awaiting_input = None
                            self.limit_start_balance = None
                            self.full_reset_to_scanning("إيقاف يدوي")
                        elif data == "cmd_status":
                            self.awaiting_input = None
                        elif data == "toggle_strict_chess":
                            self.strict_chess_enabled = not self.strict_chess_enabled
                            self._save_bot_state()
                        elif data == "toggle_sixth_candle":
                            self.sixth_candle_enabled = not self.sixth_candle_enabled
                            self._save_bot_state()
                        elif data == "toggle_limits_master":
                            self.limits_enabled = not self.limits_enabled
                            if self.limits_enabled and self.is_running:
                                self.capture_start_balance_on_run()
                            elif not self.limits_enabled:
                                self.limit_start_balance = None
                            self._save_bot_state()
                        elif data == "menu_limits":
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_limits_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_limits_keyboard()})
                            continue
                        elif data == "limits_toggle_enable":
                            self.limits_enabled = not self.limits_enabled
                            if self.limits_enabled and self.is_running:
                                self.capture_start_balance_on_run()
                            elif not self.limits_enabled:
                                self.limit_start_balance = None
                            self._save_bot_state()
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_limits_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_limits_keyboard()})
                            continue
                        elif data == "limits_reset_base":
                            self.capture_start_balance_on_run()
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_limits_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_limits_keyboard()})
                            continue
                        elif data == "set_profit_limit":
                            self.awaiting_input = "PROFIT_LIMIT"
                            await self.send_admin("📈 أرسل الآن <b>حد الربح بالدولار</b> (مثال: <code>50</code>):")
                            continue
                        elif data == "set_loss_limit":
                            self.awaiting_input = "LOSS_LIMIT"
                            await self.send_admin("📉 أرسل الآن <b>حد الخسارة بالدولار</b> (مثال: <code>50</code>):")
                            continue
                        elif data == "menu_boost":
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_boost_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_boost_keyboard()})
                            continue
                        elif data == "boost_toggle_enable":
                            self.boost_enabled = not self.boost_enabled
                            if self.boost_enabled and self.account_balance is not None and self.peak_balance is None:
                                self.peak_balance = float(self.account_balance)
                            if not self.boost_enabled:
                                self.reset_boost_state(reset_peak=False)
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_boost_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_boost_keyboard()})
                            continue
                        elif data == "boost_toggle_mode":
                            self.boost_mode = "TRADES" if self.boost_mode == "CAPITAL" else "CAPITAL"
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_boost_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_boost_keyboard()})
                            continue
                        elif data == "boost_reset_now":
                            self.reset_boost_state(reset_peak=True)
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_boost_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_boost_keyboard()})
                            continue
                        elif data == "set_boost_wins":
                            self.awaiting_input = "BOOST_WINS"
                            await self.send_admin("🏆 أرسل عدد الصفقات الناجحة المطلوبة للتعزيز (مثال: <code>5</code>):")
                            continue
                        elif data == "toggle_indicators_master":
                            self.indicators_enabled = not self.indicators_enabled
                        elif data == "menu_indicators":
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_indicators_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_indicators_keyboard()})
                            continue
                        elif data.startswith("ind_toggle_"):
                            t = data.replace("ind_toggle_", "")
                            if t == "master":
                                self.indicators_enabled = not self.indicators_enabled
                            elif t == "bb":
                                self.ind_bb_enabled = not self.ind_bb_enabled
                            elif t == "rsi":
                                self.ind_rsi_enabled = not self.ind_rsi_enabled
                            elif t == "wick":
                                self.ind_wick_enabled = not self.ind_wick_enabled
                            elif t == "stoch":
                                self.ind_stoch_enabled = not self.ind_stoch_enabled
                            elif t == "atr":
                                self.ind_atr_enabled = not self.ind_atr_enabled
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_indicators_menu_text(), "parse_mode": "HTML", "reply_markup": self.build_indicators_keyboard()})
                            continue
                        elif data == "ind_set_bb":
                            self.awaiting_input = "IND_BB_ZONE"
                            await self.send_admin("📈 أرسل حساسية البولينجر باند من 40 إلى 110 (مثال: <code>70</code>):")
                            continue
                        elif data == "ind_set_rsi":
                            self.awaiting_input = "IND_RSI_LEVELS"
                            await self.send_admin("📉 أرسل مستويات RSI للشراء والبيع (مثال: <code>38 62</code>):")
                            continue
                        elif data == "toggle_fast_mode":
                            self.fast_mode = not self.fast_mode
                        elif data == "toggle_martingale":
                            self.martingale_enabled = not self.martingale_enabled
                            if not self.martingale_enabled:
                                self.global_step_index = 0
                                self.pending_martingale_steps.clear()
                        elif data == "set_dual_pairs":
                            self.awaiting_input = "DUAL_PAIRS"
                            await self.send_admin("👯 أرسل عدد العملات الممسوكة في المزدوج (مثال: <code>3</code>):")
                            continue
                        elif data == "set_concurrent_trades":
                            self.awaiting_input = "CONCURRENT_TRADES"
                            await self.send_admin(f"🎯 أرسل عدد الصفقات المتزامنة (من 1 إلى {self.max_locked_pairs}):")
                            continue
                        elif data == "toggle_account_mode":
                            if self.is_demo_mode:
                                acc = self.get_active_account()
                                if not acc.get("has_dedicated_live"):
                                    await self.tg_api("editMessageText", {
                                        "chat_id": ADMIN_CHAT_ID, "message_id": msg_id,
                                        "text": "⚠️ <b>جلسة الحساب الحقيقي غير مضبوطة!</b> اضغط على تحديث جلسة الحقيقي وأرسل كود <code>isDemo:0</code>.",
                                        "parse_mode": "HTML",
                                        "reply_markup": {"inline_keyboard": [[{"text": "🔥 تحديث جلسة الحقيقي", "callback_data": "set_live_auth"}], [{"text": "🔙 رجوع", "callback_data": "cmd_status"}]]}
                                    })
                                    continue
                                await self.tg_api("editMessageText", {
                                    "chat_id": ADMIN_CHAT_ID, "message_id": msg_id,
                                    "text": f"⚠️ هل أنت متأكد من تفعيل <b>الحساب الحقيقي (Live)</b>؟",
                                    "parse_mode": "HTML",
                                    "reply_markup": {"inline_keyboard": [[{"text": "🔥 نعم، تفعيل الحقيقي", "callback_data": "confirm_live_mode"}], [{"text": "❌ إلغاء", "callback_data": "cmd_status"}]]}
                                })
                                continue
                            else:
                                self.is_demo_mode = True
                                self.account_balance = None
                                self.limit_start_balance = None
                                self.reset_boost_state(reset_peak=True)
                                self._sync_active_auth_frame()
                                self._save_accounts_storage()
                                self.full_reset_to_scanning("تحويل للديمو")
                                if self.ws:
                                    await self.ws.close()
                        elif data == "confirm_live_mode":
                            self.is_demo_mode = False
                            self.account_balance = None
                            self.limit_start_balance = None
                            self.reset_boost_state(reset_peak=True)
                            self._sync_active_auth_frame()
                            self._save_accounts_storage()
                            self.full_reset_to_scanning("تحويل للحقيقي")
                            if self.ws:
                                await self.ws.close()
                        elif data == "menu_accounts":
                            acc = self.get_active_account()
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID, "message_id": msg_id,
                                "text": f"👤 <b>إدارة الحسابات</b>\nالحساب النشط: <b>{html.escape(acc['name'])}</b> (<code>{acc['uid']}</code>)",
                                "parse_mode": "HTML", "reply_markup": self.build_accounts_keyboard()
                            })
                            continue
                        elif data == "set_demo_auth":
                            self.awaiting_input = "SET_DEMO_AUTH"
                            await self.send_admin('🧪 أرسل الآن كود جلسة <b>الديمو (Demo)</b> (<code>42["auth",...]</code>):')
                            continue
                        elif data == "set_live_auth":
                            self.awaiting_input = "SET_LIVE_AUTH"
                            await self.send_admin('🔥 أرسل الآن كود جلسة <b>الحقيقي (Live)</b> (<code>42["auth",...]</code>):')
                            continue
                        elif data.startswith("select_acc_"):
                            idx = int(data.replace("select_acc_", ""))
                            if 0 <= idx < len(self.accounts):
                                self.active_account_idx = idx
                                self.account_balance = None
                                self.limit_start_balance = None
                                self.reset_boost_state(reset_peak=True)
                                self._sync_active_auth_frame()
                                self._save_accounts_storage()
                                self.full_reset_to_scanning("تغيير الحساب")
                                if self.ws:
                                    await self.ws.close()
                        elif data == "add_account_start":
                            self.awaiting_input = "ADD_ACC_NAME"
                            await self.send_admin("👤 أرسل اسم الحساب الجديد:")
                            continue
                        elif data == "menu_delete_account":
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": "🗑️ اختر الحساب لحذفه:", "reply_markup": self.build_delete_accounts_keyboard()})
                            continue
                        elif data.startswith("del_acc_"):
                            idx = int(data.replace("del_acc_", ""))
                            if len(self.accounts) > 1 and 0 <= idx < len(self.accounts):
                                self.accounts.pop(idx)
                                self.active_account_idx = 0
                                self._sync_active_auth_frame()
                                self._save_accounts_storage()
                                if self.ws:
                                    await self.ws.close()
                        elif data == "toggle_boost":
                            self.boost_enabled = not self.boost_enabled
                            if not self.boost_enabled:
                                self.reset_boost_state(reset_peak=False)
                        elif data == "toggle_cont":
                            self.continuation_enabled = not self.continuation_enabled
                        elif data == "stats_preview":
                            await self.send_admin(self.format_daily_stats_report())
                            continue
                        elif data == "stats_publish":
                            await self.send_channel(self.format_daily_stats_report())
                            await self.send_admin("✅ تم نشر التقرير في القناة!")
                            continue
                        elif data == "menu_scope":
                            await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": "🔭 اختر نطاق المسح:", "reply_markup": self.build_scope_keyboard()})
                            continue
                        elif data.startswith("scope_"):
                            self.scan_scope = data.replace("scope_", "")
                            if self.strategy_mode in ("AI", "INDICATORS"):
                                asyncio.create_task(self.subscribe_all_eligible_pairs())
                        elif data == "set_amount":
                            self.awaiting_input = "BASE_AMOUNT"
                            await self.send_admin("💵 أرسل المبلغ الأساسي (مثال: <code>10</code>):")
                            continue
                        elif data == "set_boost_inc":
                            self.awaiting_input = "BOOST_INC"
                            await self.send_admin("🛡️ أرسل مقدار زيادة التعزيز (مثال: <code>10</code>):")
                            continue
                        elif data == "set_global_steps":
                            self.awaiting_input = "GLOBAL_STEPS"
                            await self.send_admin("🌐 أرسل عدد التدبيلات العام (مثال: <code>6</code>):")
                            continue
                        elif data == "set_pair_steps":
                            self.awaiting_input = "PAIR_STEPS"
                            await self.send_admin("🏛 أرسل عدد تدبيلات الزوج الواحد (مثال: <code>4</code>):")
                            continue
                        elif data == "set_payout":
                            self.awaiting_input = "PAYOUT"
                            await self.send_admin("📈 أرسل الحد الأدنى للعائد % (مثال: <code>90</code>):")
                            continue
                        elif data == "set_lead":
                            self.awaiting_input = "LEAD_TIME"
                            await self.send_admin("⏱ أرسل التقديم قبل الإغلاق بالثواني (مثال: <code>0.1</code>):")
                            continue

                        await self.tg_api("editMessageText", {"chat_id": ADMIN_CHAT_ID, "message_id": msg_id, "text": self.format_dashboard_text(), "parse_mode": "HTML", "reply_markup": self.build_main_keyboard()})

                    elif "message" in update:
                        msg = update["message"]
                        if str(msg["chat"]["id"]) != str(ADMIN_CHAT_ID):
                            continue
                        text = msg.get("text", "").strip()
                        if not text:
                            continue

                        if text in ("/start", "/menu", "القائمة"):
                            self.awaiting_input = None
                            await self.send_admin(self.format_dashboard_text(), self.build_main_keyboard())
                            continue
                        if text in ("/stats", "الاحصائيات", "الإحصائيات"):
                            await self.send_admin(self.format_daily_stats_report())
                            continue

                        if self.awaiting_input == "AI_CONFIDENCE":
                            try:
                                v = float(text.replace("%", ""))
                                if 65 <= v <= 95:
                                    self.ai_min_confidence = v / 100.0
                                    self.ai_scorer.min_confidence = self.ai_min_confidence
                                    self._save_bot_state()
                                    self.awaiting_input = None
                                    await self.send_admin(f"✅ تم ضبط ثقة AI إلى <b>{int(v)}%</b>.", self.build_main_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "SET_DEMO_AUTH":
                            ok, uid = self.update_active_account_single_mode(text, True)
                            if ok:
                                self.awaiting_input = None
                                await self.send_admin(f"✅ تم تحديث جلسة الديمو (UID: <code>{uid}</code>)", self.build_main_keyboard())
                                if self.ws and self.is_demo_mode:
                                    await self.ws.close()
                        elif self.awaiting_input == "SET_LIVE_AUTH":
                            ok, uid = self.update_active_account_single_mode(text, False)
                            if ok:
                                self.awaiting_input = None
                                await self.send_admin(f"🔥 تم تحديث جلسة الحقيقي (UID: <code>{uid}</code>)", self.build_main_keyboard())
                                if self.ws and not self.is_demo_mode:
                                    await self.ws.close()
                        elif self.awaiting_input == "ADD_ACC_NAME":
                            self.temp_new_acc_name = text[:30]
                            self.awaiting_input = "ADD_ACC_DEMO"
                            await self.send_admin('🧪 أرسل كود جلسة الديمو (<code>42["auth",...]</code>):')
                        elif self.awaiting_input == "ADD_ACC_DEMO":
                            p = parse_auth_frame(text)
                            if p and p.get("session"):
                                uid = str(p.get("uid", "Unknown"))
                                self.accounts.append(self._normalize_account_record({"name": self.temp_new_acc_name, "uid": uid, "demo_auth_frame": text.strip()}))
                                self.active_account_idx = len(self.accounts) - 1
                                self._sync_active_auth_frame()
                                self._save_accounts_storage()
                                self.awaiting_input = None
                                await self.send_admin(f"✅ تم إضافة الحساب <b>{html.escape(self.temp_new_acc_name)}</b>!", self.build_main_keyboard())
                                if self.ws:
                                    await self.ws.close()
                        elif self.awaiting_input == "PROFIT_LIMIT":
                            try:
                                self.profit_limit = max(0.0, float(text.replace("$", "")))
                                self._save_bot_state()
                                self.awaiting_input = None
                                await self.send_admin(f"✅ حد الربح: <b>+${self.profit_limit:g}</b>", self.build_limits_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "LOSS_LIMIT":
                            try:
                                self.loss_limit = max(0.0, float(text.replace("$", "")))
                                self._save_bot_state()
                                self.awaiting_input = None
                                await self.send_admin(f"✅ حد الخسارة: <b>-${self.loss_limit:g}</b>", self.build_limits_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "BOOST_WINS":
                            try:
                                self.boost_target_wins = max(1, int(text))
                                self.awaiting_input = None
                                await self.send_admin(f"✅ هدف التعزيز: <b>{self.boost_target_wins}</b> صفقات", self.build_boost_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "IND_BB_ZONE":
                            try:
                                self.ind_bb_zone = float(text.replace("%", "")) / 100.0
                                self.awaiting_input = None
                                await self.send_admin("✅ تم تحديث حساسية BB", self.build_indicators_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "IND_RSI_LEVELS":
                            try:
                                parts = text.replace("-", " ").split()
                                self.ind_rsi_oversold, self.ind_rsi_overbought = float(parts[0]), float(parts[1])
                                self.awaiting_input = None
                                await self.send_admin("✅ تم تحديث مستويات RSI", self.build_indicators_keyboard())
                            except Exception:
                                pass
                        elif self.awaiting_input == "DUAL_PAIRS":
                            try:
                                self.max_locked_pairs = max(1, min(10, int(text)))
                                self.max_concurrent_trades = min(self.max_concurrent_trades, self.max_locked_pairs)
                                self.awaiting_input = None
                                await self.send_admin(f"✅ المزدوج: <b>{self.max_locked_pairs}</b> عملات", self.build_main_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "CONCURRENT_TRADES":
                            try:
                                v = int(text)
                                if 1 <= v <= self.max_locked_pairs:
                                    self.max_concurrent_trades = v
                                    self.awaiting_input = None
                                    await self.send_admin(f"✅ الصفقات المتزامنة: <b>{v}</b>", self.build_main_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "BASE_AMOUNT":
                            try:
                                self.base_amount = max(1.0, float(text))
                                if not self.in_boost_recovery:
                                    self.effective_base_amount = self.base_amount
                                self.awaiting_input = None
                                await self.send_admin(f"✅ الأساسي: <b>${self.base_amount:g}</b>", self.build_main_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "BOOST_INC":
                            try:
                                self.boost_increment = max(1.0, float(text))
                                self.awaiting_input = None
                                await self.send_admin(f"✅ زيادة التعزيز: <b>+${self.boost_increment:g}</b>", self.build_boost_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "GLOBAL_STEPS":
                            try:
                                self.global_steps = max(1, min(12, int(text)))
                                self.awaiting_input = None
                                await self.send_admin(f"✅ التدبيل العام: <b>{self.global_steps}</b>", self.build_main_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "PAIR_STEPS":
                            try:
                                self.pair_steps = max(1, min(12, int(text)))
                                self.awaiting_input = None
                                await self.send_admin(f"✅ تدبيل الزوج: <b>{self.pair_steps}</b>", self.build_main_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "PAYOUT":
                            try:
                                self.min_payout = max(50, min(100, int(float(text.replace("%", "")))))
                                self.awaiting_input = None
                                await self.send_admin(f"✅ العائد المطلوب: <b>≥ {self.min_payout}%</b>", self.build_main_keyboard())
                            except ValueError:
                                pass
                        elif self.awaiting_input == "LEAD_TIME":
                            try:
                                self.entry_lead_seconds = float(text)
                                self.awaiting_input = None
                                await self.send_admin(f"✅ التقديم: <b>{self.entry_lead_seconds}ث</b>", self.build_main_keyboard())
                            except ValueError:
                                pass
            except Exception:
                await asyncio.sleep(2)

    # ==========================================
    # تحديث بيانات السوق والشموع
    # ==========================================
    def update_assets_payouts(self, assets_list: list):
        curr_list, crypto_list, stocks_list = [], [], []
        crypto_keys = ("BTC", "ETH", "LTC", "XRP", "SOL", "BNB", "DOGE", "ADA", "DOT", "AVAX", "LINK", "MATIC", "TON", "TRON", "TRX")
        for item in assets_list:
            if not isinstance(item, list) or len(item) < 6:
                continue
            symbol = str(item[1])
            disp_name = str(item[2]).strip() if len(item) > 2 and item[2] else symbol
            asset_type = str(item[3]).strip().lower() if len(item) > 3 else ""
            try:
                payout = int(float(item[5]))
            except (ValueError, TypeError):
                continue
            is_open = bool(item[14]) if len(item) > 14 else True
            self.payouts[symbol] = payout
            self.asset_display_names[symbol] = disp_name
            if is_open:
                self.open_assets.add(symbol)
            else:
                self.open_assets.discard(symbol)
                continue
            u_sym, u_name = symbol.upper(), disp_name.upper()
            if "crypto" in asset_type or any(u_sym.startswith(k) or k in u_name for k in crypto_keys):
                crypto_list.append(symbol)
            elif "stock" in asset_type or symbol.startswith("#"):
                stocks_list.append(symbol)
            elif "currency" in asset_type or (symbol.endswith("_otc") and len(symbol) == 10):
                curr_list.append(symbol)

        if curr_list:
            self.assets_by_cat["CURRENCIES"] = curr_list
        if crypto_list:
            self.assets_by_cat["CRYPTO"] = crypto_list
        if stocks_list:
            self.assets_by_cat["STOCKS"] = stocks_list
        if self.strategy_mode in ("AI", "INDICATORS") and self.authenticated:
            asyncio.create_task(self.subscribe_all_eligible_pairs())

    def is_pair_eligible(self, asset: str) -> tuple[bool, int]:
        payout = self.payouts.get(asset)
        if payout is None or (self.open_assets and asset not in self.open_assets):
            return False, 0
        return (payout >= self.min_payout), payout

    def update_candle_from_history(self, asset: str, rows: list):
        if not rows:
            return
        self.candles.setdefault(asset, {})
        for row in rows:
            if isinstance(row, list) and len(row) >= 3:
                m_ts = (int(normalize_ts(row[0])) // 60) * 60
                o_p, c_p = float(row[1]), float(row[2])
                h_p = float(row[3]) if len(row) >= 5 else max(o_p, c_p)
                l_p = float(row[4]) if len(row) >= 5 else min(o_p, c_p)
                self.candles[asset][m_ts] = {"open": o_p, "close": c_p, "high": h_p, "low": l_p}
            elif isinstance(row, dict) and "time" in row:
                m_ts = (int(normalize_ts(row["time"])) // 60) * 60
                o_p, c_p = float(row["open"]), float(row["close"])
                self.candles[asset][m_ts] = {"open": o_p, "close": c_p, "high": float(row.get("high", max(o_p, c_p))), "low": float(row.get("low", min(o_p, c_p)))}

    def update_candle_from_tick(self, asset: str, raw_ts, price: float):
        self.candles.setdefault(asset, {})
        ts = normalize_ts(raw_ts)
        self.server_hour_shift = round((ts - time.time()) / 3600.0) * 3600.0
        self.latest_prices[asset] = price
        m_ts = (int(ts) // 60) * 60
        bucket = self.candles[asset].get(m_ts)
        if bucket is None:
            self.candles[asset][m_ts] = {"open": price, "close": price, "high": price, "low": price}
        else:
            bucket["close"] = price
            bucket["high"] = max(bucket.get("high", price), price)
            bucket["low"] = min(bucket.get("low", price), price)
        if len(self.candles[asset]) > 140:
            for k in sorted(self.candles[asset].keys())[:-120]:
                del self.candles[asset][k]

    def get_last_five(self, asset: str):
        pc = self.candles.get(asset, {})
        sm = sorted(pc.keys())
        if len(sm) < 5:
            return None
        m4, m3, m2, m1, m_open = sm[-5:]
        return (m4, m3, m2, m1, m_open), tuple(candle_color(pc[m]["open"], pc[m]["close"]) for m in (m4, m3, m2, m1, m_open))

    def get_last_six(self, asset: str):
        pc = self.candles.get(asset, {})
        sm = sorted(pc.keys())
        if len(sm) < 6:
            return None
        m5, m4, m3, m2, m1, m_open = sm[-6:]
        return (m5, m4, m3, m2, m1, m_open), tuple(candle_color(pc[m]["open"], pc[m]["close"]) for m in (m5, m4, m3, m2, m1, m_open))

    # ==========================================
    # فتح الصفقات ومراقبة الإغلاق والتدبيل
    # ==========================================
    async def open_trade(
        self, asset: str, direction: str, g_idx: int, p_idx: int, trade_minute: int,
        signal_uid: int | None, is_draw_retry: bool = False, is_cont_entry: bool = False,
        cycle_base: float | None = None, custom_signal_header: str | None = None
    ):
        if not self.ws or not self.authenticated or not self.is_running:
            return
        if self.account_balance is not None and self.check_and_enforce_limits(float(self.account_balance)):
            return

        if cycle_base is None:
            cycle_base = self.get_current_entry_base()
        ladder = self.get_trade_ladder(custom_base=cycle_base)
        g_idx = 0 if not self.martingale_enabled else (g_idx % len(ladder))
        amount = ladder[g_idx]

        await self.ensure_subscribed(asset)
        self.trade_seq += 1
        entry_price = self.latest_prices.get(asset)
        close_ts = self.get_server_now() + TRADE_DURATION + MARTINGALE_OFFSET
        sig_sym = self.format_signal_symbol(asset)
        dir_text = "🟢 CALL UP 📈" if direction == "call" else "🔴 PUT DOWN 📉"

        if custom_signal_header and signal_uid is None:
            now_str = datetime.now(tz=TZ_UTC3).strftime("%H:%M:%S")
            signal_uid = self.enqueue_new_signal(
                f"╭━━━[ 🛡 عبود تريدينج ]━╮\n┃ {custom_signal_header}\n┃ 🏛 <code>{sig_sym}</code>\n"
                f"┃ ⏰ Entry  <code>{now_str}</code> [UTC+3]\n┃ ⏱ Expiry  M1\n┃ {dir_text}\n╰━━━━━━━━━━━━━━━╯"
            )
        elif is_cont_entry:
            signal_uid = self.enqueue_new_signal(
                f"╭━━━[ 🛡 عبود تريدينج ]━╮\n┃ 🔄 <b>مواصلة النموذج</b>\n┃ 🏛 <code>{sig_sym}</code>\n┃ {dir_text}\n╰━━━━━━━━━━━━━━━╯"
            )
        elif p_idx == 0 and not is_draw_retry:
            self.enqueue_signal_reply(signal_uid, f"🚀 <b>تم تأكيد الدخول في الصفقة ✅</b>\n🏛 <code>{sig_sym}</code> | {dir_text}")

        self.active_trades.append({
            "trade_id": self.trade_seq, "asset": asset, "direction": direction,
            "g_idx": g_idx, "p_idx": p_idx, "trade_minute": trade_minute,
            "close_ts": close_ts, "entry_price": entry_price, "price_confirmed": False,
            "amount": amount, "cycle_base": cycle_base, "signal_uid": signal_uid, "is_cont": is_cont_entry
        })
        self.refresh_mode_state()
        self.total_turnover += amount
        self.current_cycle_spent += amount
        self._save_bot_state()

        order_payload = {
            "asset": asset, "amount": amount, "action": direction,
            "isDemo": 1 if self.is_demo_mode else 0,
            "requestId": int(time.time() * 1000) + self.trade_seq,
            "optionType": 100, "time": TRADE_DURATION
        }
        await self.ws.send(f'42["openOrder",{json.dumps(order_payload)}]')
        log("TRADE_EXEC", f"⚡ تنفيذ | {sig_sym} | {direction.upper()} | ${amount} | [عام {g_idx + 1}/{len(ladder)} | زوج {p_idx + 1}/{self.pair_steps}]")

    def evaluate_trade_result(self, trade: dict) -> str:
        curr_p = self.latest_prices.get(trade["asset"])
        entry_p = trade["entry_price"]
        if curr_p is not None and entry_p is not None:
            if curr_p == entry_p:
                return "DRAW"
            return ("WIN" if curr_p < entry_p else "LOSS") if trade["direction"] == "put" else ("WIN" if curr_p > entry_p else "LOSS")
        return "LOSS"

    async def precision_entry_watcher(self):
        while True:
            server_now = self.get_server_now()
            self.ai_scorer.resolve_shadow_samples(self.latest_prices, server_now)

            if not self.is_running or (not self.locked_candidates and not self.active_trades):
                await asyncio.sleep(0.05)
                continue

            if self.account_balance is not None and self.check_and_enforce_limits(float(self.account_balance)):
                await asyncio.sleep(0.05)
                continue

            deadlines = []
            if self.active_trades:
                deadlines.append(min(t["close_ts"] for t in self.active_trades))
            if self.locked_candidates:
                deadlines.append(min(c["target_minute"] for c in self.locked_candidates) + 60.0 + SERVER_CLOCK_OFFSET)

            min_rem = min(deadlines) - server_now
            if min_rem > 2.0:
                await asyncio.sleep(0.05)
                continue
            elif min_rem > self.entry_lead_seconds:
                await asyncio.sleep(0.001)
                continue

            if self.active_trades:
                for trade in [t for t in self.active_trades if (t["close_ts"] - server_now) <= self.entry_lead_seconds]:
                    if not self.is_running:
                        break
                    if trade in self.active_trades:
                        self.active_trades.remove(trade)

                    asset, sig_sym, sig_uid = trade["asset"], self.format_signal_symbol(trade["asset"]), trade["signal_uid"]
                    direction, g_idx, p_idx = trade["direction"], trade["g_idx"], trade["p_idx"]
                    won_amount, trade_cycle_base, is_cont = trade["amount"], trade["cycle_base"], trade["is_cont"]
                    result = self.evaluate_trade_result(trade)
                    attempt_num = g_idx + 1
                    next_minute = (trade["trade_minute"] or int(server_now // 60) * 60) + 60

                    if result == "WIN":
                        self.record_trade_outcome(sig_sym, True, attempt_num)
                        self.enqueue_signal_reply(sig_uid, f"╭━━━[ 🛡 عبود تريدينج ]━╮\n┃ 🏛 <code>{sig_sym}</code>\n┃ 🏆 <b>WIN{to_superscript(attempt_num)} ✅</b>\n╰━━━━━━━━━━━━━━━╯")
                        if not any(t["g_idx"] > 0 for t in self.active_trades):
                            self.global_step_index = 0
                        self.current_cycle_spent = 0.0

                        if self.limits_enabled and self.limit_start_balance is not None and self.account_balance is not None and self.profit_limit > 0:
                            est_bal = float(self.account_balance) + won_amount + (won_amount * (self.payouts.get(asset, self.min_payout) / 100.0))
                            if self.check_and_enforce_limits(est_bal):
                                break

                        if self.boost_enabled and self.in_boost_recovery and self.boost_mode == "TRADES":
                            self.boost_wins_done += 1
                            if self.boost_wins_done >= self.boost_target_wins:
                                self.reset_boost_state(reset_peak=False)

                        if self.continuation_enabled and attempt_num == 1 and not is_cont and self.is_running:
                            if self.is_pair_eligible(asset)[0]:
                                await self.open_trade(asset, "call" if direction == "put" else "put", 0, 0, next_minute, None, is_cont_entry=True)
                        continue

                    if result == "DRAW":
                        self.current_cycle_spent -= won_amount
                        await self.open_trade(asset, direction, g_idx, p_idx, next_minute, sig_uid, is_draw_retry=True, is_cont_entry=is_cont, cycle_base=trade_cycle_base)
                        continue

                    if self.account_balance is not None and self.check_and_enforce_limits(float(self.account_balance)):
                        break

                    if not self.martingale_enabled or (g_idx + 1) >= self.global_steps:
                        self.record_trade_outcome(sig_sym, False, attempt_num)
                        self.enqueue_signal_reply(sig_uid, f"╭━━━[ 🛡 عبود تريدينج ]━╮\n┃ 🏛 <code>{sig_sym}</code>\n┃ ❌ <b>LOSS</b>\n╰━━━━━━━━━━━━━━━╯")
                        if self.boost_enabled and not self.in_boost_recovery:
                            self.in_boost_recovery = True
                            self.boost_wins_done = 0
                            self.effective_base_amount = self.base_amount + self.boost_increment
                        self.current_cycle_spent = 0.0
                        self.global_step_index = 0
                    else:
                        next_g, next_p = g_idx + 1, p_idx + 1
                        if next_p < self.pair_steps and self.is_running:
                            self.global_step_index = next_g
                            await self.open_trade(asset, direction, next_g, next_p, next_minute, sig_uid, is_cont_entry=is_cont, cycle_base=trade_cycle_base)
                        else:
                            self.global_step_index = next_g
                            self.pending_martingale_steps.append((next_g, trade_cycle_base))

            if self.locked_candidates and self.is_running:
                earliest_minute = min(c["target_minute"] for c in self.locked_candidates)
                if ((earliest_minute + 60.0 + SERVER_CLOCK_OFFSET) - server_now) <= self.entry_lead_seconds:
                    due = [c for c in self.locked_candidates if c["target_minute"] <= earliest_minute]
                    future = [c for c in self.locked_candidates if c["target_minute"] > earliest_minute]
                    selected = []
                    max_open = min(self.max_concurrent_trades, self.max_locked_pairs)

                    for cand in due:
                        if len(selected) >= max_open:
                            self.enqueue_signal_reply(cand["signal_uid"], f"⚪ <b>تم إلغاء الإشارة</b>\n🏛 <code>{self.format_signal_symbol(cand['asset'])}</code>")
                            continue
                        tc = self.candles.get(cand["asset"], {}).get(cand["target_minute"])
                        if tc and self.is_pair_eligible(cand["asset"])[0]:
                            col = candle_color(tc["open"], tc["close"])
                            ok = (cand["direction"] == "put" and col == "GREEN") or (cand["direction"] == "call" and col == "RED")
                            if ok and self.indicators_enabled:
                                ok, _ = self.check_indicators(cand["asset"], cand["direction"], is_pre_close=True)
                            if ok:
                                selected.append(cand)
                                continue
                        self.enqueue_signal_reply(cand["signal_uid"], f"⚪ <b>تم إلغاء الإشارة</b>\n🏛 <code>{self.format_signal_symbol(cand['asset'])}</code>")

                    if selected:
                        for o in future:
                            self.enqueue_signal_reply(o["signal_uid"], f"⚪ <b>تم إلغاء الإشارة</b>\n🏛 <code>{self.format_signal_symbol(o['asset'])}</code>")
                        self.locked_candidates.clear()
                        for cand in selected:
                            if self.martingale_enabled and self.pending_martingale_steps:
                                st, cb = self.pending_martingale_steps.pop(0)
                            elif self.martingale_enabled and not any(t["g_idx"] > 0 for t in self.active_trades):
                                st, cb = self.global_step_index, None
                            else:
                                st, cb = 0, None
                            await self.open_trade(cand["asset"], cand["direction"], st, 0, cand["target_minute"] + 60, cand["signal_uid"], cycle_base=cb)
                    else:
                        self.locked_candidates = future
                        if not future and not self.active_trades:
                            self.switch_pair_keep_global("لم تستوفِ أي عملة الشرط")

            self.refresh_mode_state()

    # ==========================================
    # تشغيل الاستراتيجيات الثلاث على السوق الحي
    # ==========================================
    async def process_realtime_tick_strategies(self, asset: str, price: float, now_ts: float):
        if asset not in self.get_active_scan_pairs() or not self.is_pair_eligible(asset)[0]:
            return

        pair_candles = self.candles.get(asset, {})
        ai_dir, ai_conf, engine_lbl, meta = self.ai_scorer.evaluate_tick(asset, price, now_ts, pair_candles)

        if not self.is_running:
            return
        if self.account_balance is not None and self.check_and_enforce_limits(float(self.account_balance)):
            return
        if asset in {t["asset"] for t in self.active_trades} or len(self.active_trades) >= min(self.max_concurrent_trades, self.max_locked_pairs):
            return

        if self.strategy_mode == "AI" and ai_dir is not None:
            # تدقيق إضافي عبر Gemini 2.5 Flash المجاني إذا كان المفتاح مضافًا
            if GEMINI_API_KEY:
                gem_ok, gem_reason = await self.verify_with_gemini_free(asset, ai_dir, meta)
                if not gem_ok:
                    log("GEMINI_SKIP", f"✨ رفض Gemini فرصة {asset} ({ai_dir}): {gem_reason}")
                    return
                engine_lbl = f"{engine_lbl} + Gemini"

            st, cb = (self.pending_martingale_steps.pop(0) if (self.martingale_enabled and self.pending_martingale_steps) else ((self.global_step_index, None) if (self.martingale_enabled and not any(t["g_idx"] > 0 for t in self.active_trades)) else (0, None)))
            await self.open_trade(asset, ai_dir, st, 0, (int(now_ts) // 60) * 60, None, cycle_base=cb, custom_signal_header=f"🧠 <b>قنص هجين ({engine_lbl})</b> | ثقة: <b>{int(ai_conf * 100)}%</b>")
            return

        if self.strategy_mode == "INDICATORS" and now_ts >= self.ind_strategy_cooldown.get(asset, 0.0):
            ind_dir, ind_info = self.evaluate_pure_indicators_signal(asset)
            if ind_dir is not None:
                self.ind_strategy_cooldown[asset] = now_ts + 65.0
                st, cb = (self.pending_martingale_steps.pop(0) if (self.martingale_enabled and self.pending_martingale_steps) else ((self.global_step_index, None) if (self.martingale_enabled and not any(t["g_idx"] > 0 for t in self.active_trades)) else (0, None)))
                await self.open_trade(asset, ind_dir, st, 0, (int(now_ts) // 60) * 60, None, cycle_base=cb, custom_signal_header=f"📊 <b>استراتيجية المؤشرات المستقلة</b> ({ind_info})")

    async def process_market_logic(self, asset: str):
        if not self.is_running or self.strategy_mode != "CHESS":
            return
        if self.account_balance is not None and self.check_and_enforce_limits(float(self.account_balance)):
            return

        locked_assets = {c["asset"] for c in self.locked_candidates}
        active_assets = {t["asset"] for t in self.active_trades}
        if asset not in self.get_active_scan_pairs() or asset in locked_assets or asset in active_assets:
            return
        if not self.is_pair_eligible(asset)[0]:
            return
        if len(self.locked_candidates) >= self.max_locked_pairs or (not self.fast_mode and self.active_trades):
            return

        if self.sixth_candle_enabled:
            d6 = self.get_last_six(asset)
            if d6 is None:
                return
            (m5, m4, m3, m2, m1, m_open), (c5, c4, c3, c2, c1, _) = d6
            seq = (m5, m4, m3, m2, m1, m_open)
            is_put = (c5 == "GREEN" and c4 == "RED" and c3 == "RED" and c2 == "GREEN" and c1 == "RED")
            is_call = (c5 == "RED" and c4 == "GREEN" and c3 == "GREEN" and c2 == "RED" and c1 == "GREEN")
        else:
            d5 = self.get_last_five(asset)
            if d5 is None:
                return
            (m4, m3, m2, m1, m_open), (c4, c3, c2, c1, _) = d5
            seq = (m4, m3, m2, m1, m_open)
            is_put = (c4 == "RED" and c3 == "RED" and c2 == "GREEN" and c1 == "RED")
            is_call = (c4 == "GREEN" and c3 == "GREEN" and c2 == "RED" and c1 == "GREEN")

        if (m_open + 60.0 + SERVER_CLOCK_OFFSET - self.get_server_now()) <= (self.entry_lead_seconds + 0.5):
            return

        direction = "put" if is_put else ("call" if is_call else None)
        if direction is None:
            return

        if not self.check_strict_chess_candles(asset, seq)[0]:
            return
        if self.indicators_enabled and not self.check_indicators(asset, direction, is_pre_close=False)[0]:
            return

        sig_sym = self.format_signal_symbol(asset)
        entry_str = datetime.fromtimestamp((m_open + 60) - self.server_hour_shift, tz=TZ_UTC3).strftime("%H:%M:%S")
        rank = len(self.locked_candidates) + 1
        dir_txt = "🔴 <b>PUT DOWN 📉</b>\n┃ 📌 إذا أغلقت خضراء 🟢 ⬅️ بيع" if direction == "put" else "🟢 <b>CALL UP 📈</b>\n┃ 📌 إذا أغلقت حمراء 🔴 ⬅️ شراء"

        sig_uid = self.enqueue_new_signal(
            f"╭━━━[ 🛡 عبود تريدينج ]━╮\n┃ 🏛 <code>{sig_sym}</code>\n┃ ⏰ Entry  <code>{entry_str}</code> [UTC+3]\n┃ ⏱ Expiry  M1\n┃ {dir_txt}\n╰━━━━━━━━━━━━━━━╯"
        )
        self.locked_candidates.append({"asset": asset, "direction": direction, "target_minute": m_open, "signal_uid": sig_uid, "rank": rank})
        self.refresh_mode_state()
        asyncio.create_task(self.ensure_subscribed(asset))

    # ==========================================
    # الاتصال بالسوق وخادم الويب
    # ==========================================
    async def handle_payload(self, event_name: str, data):
        if event_name == "successauth":
            self.authenticated = True
            log("AUTH", "تم تأكيد المصادقة بنجاح ✅")
            if self.strategy_mode in ("AI", "INDICATORS"):
                asyncio.create_task(self.subscribe_all_eligible_pairs())
        elif event_name == "updateAssets" and isinstance(data, list):
            self.update_assets_payouts(data)
        elif event_name == "successupdateBalance" and isinstance(data, dict):
            if data.get("isDemo") is None or bool(data.get("isDemo")) == self.is_demo_mode:
                self.update_balance_and_peak(float(data.get("balance", 0)))
        elif event_name == "successopenOrder" and isinstance(data, dict):
            open_p = data.get("openPrice")
            if open_p is not None:
                for t in reversed(self.active_trades):
                    if t["asset"] == data.get("asset") and not t["price_confirmed"]:
                        t["entry_price"] = float(open_p)
                        t["price_confirmed"] = True
                        break
        elif event_name == "updateStream" and isinstance(data, list):
            for item in data:
                if isinstance(item, list) and len(item) >= 3:
                    asset, ts, price = str(item[0]), item[1], float(item[2])
                    self.update_candle_from_tick(asset, ts, price)
                    await self.process_realtime_tick_strategies(asset, price, self.get_server_now())
                    if self.strategy_mode == "CHESS":
                        await self.process_market_logic(asset)
        elif isinstance(data, dict):
            asset = data.get("asset", self.active_symbol)
            if data.get("history"):
                self.update_candle_from_history(asset, data["history"])
            if data.get("candles"):
                self.update_candle_from_history(asset, data["candles"])
            await self.process_market_logic(asset)
        elif isinstance(data, list) and data and event_name in ("history", "loadHistoryPeriod", "loadHistoryPeriodFast", "updateHistoryNew", "updateHistoryNewFast"):
            self.update_candle_from_history(self.active_symbol, data)
            await self.process_market_logic(self.active_symbol)

    async def keepalive_heartbeat_loop(self, ws):
        while True:
            await asyncio.sleep(12)
            if self.authenticated:
                try:
                    await ws.send('42["ps"]')
                except Exception:
                    break

    async def scanner_rotation_loop(self, ws):
        for _ in range(30):
            if self.authenticated:
                break
            await asyncio.sleep(0.2)
        if not self.authenticated:
            return

        idx = focus_idx = 0
        while True:
            if not self.is_running:
                await asyncio.sleep(1)
                continue

            if self.strategy_mode in ("AI", "INDICATORS"):
                await self.subscribe_all_eligible_pairs()
                pairs = self.get_active_scan_pairs()
                if pairs:
                    pair = pairs[idx % len(pairs)]
                    idx += 1
                    if self.is_pair_eligible(pair)[0] and len(self.candles.get(pair, {})) < 25:
                        self.req_index += 1
                        try:
                            await ws.send(f'42["loadHistoryPeriod",{json.dumps({"asset": pair, "index": self.req_index, "time": int(self.get_server_now()), "offset": 3600, "period": 60})}]')
                        except Exception:
                            break
                await asyncio.sleep(1.5)
                continue

            if not self.fast_mode and self.active_trades:
                t_assets = list(dict.fromkeys(t["asset"] for t in self.active_trades))
                pair = t_assets[focus_idx % len(t_assets)]
                focus_idx += 1
                try:
                    await ws.send(f'42["changeSymbol",{{"asset":"{pair}","period":60}}]')
                except Exception:
                    break
                await asyncio.sleep(1.0)
                continue

            pairs = self.get_active_scan_pairs()
            if not pairs:
                await asyncio.sleep(1)
                continue
            pair = pairs[idx % len(pairs)]
            idx += 1
            if not self.is_pair_eligible(pair)[0]:
                await asyncio.sleep(0.05)
                continue
            await self.ensure_subscribed(pair)
            self.active_symbol = pair
            self.req_index += 1
            try:
                await ws.send(f'42["changeSymbol",{{"asset":"{pair}","period":60}}]')
                await ws.send(f'42["loadHistoryPeriod",{json.dumps({"asset": pair, "index": self.req_index, "time": int(self.get_server_now()), "offset": 3600, "period": 60})}]')
            except Exception:
                break
            await asyncio.sleep(SCAN_INTERVAL_PER_PAIR)

    async def start_keepalive_web_server(self):
        app = web.Application()
        app.router.add_get("/", lambda r: web.Response(text=f"Abood Trader Alive | {self.strategy_mode}"))
        app.router.add_get("/health", lambda r: web.Response(text="OK"))
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "10000"))).start()

    async def run(self):
        headers = {"Origin": "https://pocketoption.com", "User-Agent": "Mozilla/5.0"}
        await self.start_keepalive_web_server()
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True)) as http_session:
            self.http_session = http_session
            asyncio.create_task(self.channel_sender_worker())
            asyncio.create_task(self.telegram_polling_loop())
            asyncio.create_task(self.precision_entry_watcher())
            asyncio.create_task(self.daily_midnight_stats_scheduler())
            await self.send_admin(self.format_dashboard_text(), self.build_main_keyboard())

            ws_params = inspect.signature(websockets.connect).parameters
            h_key = "additional_headers" if "additional_headers" in ws_params else "extra_headers"

            while True:
                self.authenticated = False
                self.subscribed_pairs.clear()
                try:
                    async with websockets.connect(self.get_ws_url(), **{h_key: headers, "ping_interval": None, "close_timeout": 5}) as ws:
                        self.ws = ws
                        hb = asyncio.create_task(self.keepalive_heartbeat_loop(ws))
                        sc = asyncio.create_task(self.scanner_rotation_loop(ws))
                        async for raw in ws:
                            if isinstance(raw, str):
                                if raw.startswith("0"):
                                    await ws.send("40")
                                elif raw.startswith("40"):
                                    await ws.send(self.auth_frame)
                                elif raw == "2":
                                    await ws.send("3")
                                elif raw.startswith("42"):
                                    try:
                                        p = json.loads(raw[2:])
                                        if isinstance(p, list) and len(p) >= 2:
                                            await self.handle_payload(p[0], p[1])
                                    except Exception:
                                        pass
                                elif raw.startswith("451-"):
                                    try:
                                        h = json.loads(raw[4:])
                                        self.pending_binary_event = h[0] if isinstance(h, list) and h else None
                                    except Exception:
                                        self.pending_binary_event = None
                            elif isinstance(raw, bytes) and self.pending_binary_event:
                                ev = self.pending_binary_event
                                self.pending_binary_event = None
                                try:
                                    txt = raw.decode("utf-8", errors="ignore")
                                    idxs = [i for i in (txt.find("{"), txt.find("[")) if i != -1]
                                    if idxs:
                                        await self.handle_payload(ev, json.loads(txt[min(idxs):]))
                                except Exception:
                                    pass
                        hb.cancel()
                        sc.cancel()
                        self.ws = None
                except Exception as e:
                    self.ws = None
                    log("ERROR", f"إعادة الاتصال: {e}")
                await asyncio.sleep(1.5)


if __name__ == "__main__":
    try:
        asyncio.run(AboodControlBot().run())
    except KeyboardInterrupt:
        log("STOP", "تم الإيقاف.")
