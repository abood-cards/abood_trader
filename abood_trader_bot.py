import asyncio
import inspect
import json
import os
import secrets
import sys
import time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
import aiohttp
from aiohttp import web
import websockets

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# ==========================================
# 1. إعدادات الربط والتوقيت (عدّلها بسهولة من هنا)
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "ضع_توكن_بوت_تليجرام_هنا")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "ضع_رقم_الآيدي_الخاص_بك_هنا")
CHANNEL_ID = os.getenv("CHANNEL_ID", "ضع_اي_دي_القناة_هنا")

# رابط الاستضافة العام على Render (يُقرأ تلقائيًا من Render أو من الرابط الافتراضي)
PUBLIC_BASE_URL = os.getenv("RENDER_EXTERNAL_URL", "https://abood-trader.onrender.com").rstrip("/")

# المتغير الثابت للفحص والدخول قبل إغلاق الشمعة/الصفقة بأجزاء الثانية (0.1 = 100 مللي ثانية)
DEFAULT_LEAD_SECONDS = 0.1

# تعويض فارق أجزاء الثانية بين ساعة Render وساعة سيرفر Pocket Option
SERVER_CLOCK_OFFSET = 0.75

# تأخير إضافي بأجزاء الثانية لضبط لحظة التدبيلة بدقة ومنع تقدمها ثانية كاملة
MARTINGALE_OFFSET = 0.05

DEFAULT_AUTH_FRAME = os.getenv(
    "PO_AUTH_FRAME",
    '42["auth",{"session":"et97hpjdhses78itun3rk2peld","isDemo":1,"uid":142271281,"platform":2,"isFastHistory":true,"isOptimized":true}]'
)

ACCOUNTS_FILE = "accounts.json"

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
    if close_price >= open_price:
        return "GREEN"
    return "RED"


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
# دوال حساب المؤشرات الفنية (بدون مكتبات خارجية)
# ==========================================
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


class AboodControlBot:
    def __init__(self):
        # الإعدادات الأساسية القابلة للتحكم من تليجرام
        self.is_running = False
        self.base_amount = 10.0
        self.martingale_enabled = True
        self.global_steps = 6
        self.pair_steps = 4
        self.min_payout = 90
        self.entry_lead_seconds = DEFAULT_LEAD_SECONDS

        # الوضع السريع (Turbo Mode)
        self.fast_mode = False

        # ميزة المزدوج + عدد الصفقات المتزامنة
        self.max_locked_pairs = 1
        self.max_concurrent_trades = 1
        self.locked_candidates = []

        # ==========================================
        # نظام المؤشرات والفلاتر (بإعدادات متوسطة ومتوازنة)
        # ==========================================
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

        # إدارة الحسابات ووضع التداول (تجريبي / حقيقي) + رمز الأمان لبوابة الهاتف
        self.accounts = []
        self.active_account_idx = 0
        self.is_demo_mode = True
        self.auth_frame = DEFAULT_AUTH_FRAME
        self.web_connect_token = ""
        self.temp_new_acc_name = ""
        self._load_accounts_storage()

        # ==========================================
        # نظام التعزيز الذكي (يُطبَّق على الصفقات الجديدة فقط دون المساس بالصفقات الجارية)
        # ==========================================
        self.boost_enabled = False
        self.boost_mode = "CAPITAL"
        self.boost_increment = 10.0
        self.boost_target_wins = 5
        self.boost_wins_done = 0
        self.effective_base_amount = 10.0
        self.in_boost_recovery = False
        self.peak_balance = None
        self.current_cycle_spent = 0.0

        # 2) إعدادات وضع مواصلة النموذج بعد الربح من أول دخول
        self.continuation_enabled = False

        # نطاق المسح
        self.scan_scope = "CURRENCIES"

        self.assets_by_cat = {
            "CURRENCIES": list(FALLBACK_CURRENCIES),
            "CRYPTO": list(FALLBACK_CRYPTO),
            "STOCKS": list(FALLBACK_STOCKS),
        }
        self.asset_display_names = {}
        self.awaiting_input = None

        # بيانات السوق والشموع ونسب الأرباح
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

        # نظام ربط الردود على رسائل القناة لكل صفقة (Reply Threading)
        self.channel_queue = asyncio.Queue()
        self.signal_seq = 0
        self.signal_msg_ids = {}

        # إدارة الصفقات المفتوحة
        self.mode = "SCANNING"
        self.active_trades = []
        self.trade_seq = 0
        self.global_step_index = 0
        self.pending_martingale_steps = []

        # سجل الإحصائيات اليومية + عائد التداول (Turnover)
        self.stats_date = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
        self.daily_records = []
        self.total_turnover = 0.0

    # ==========================================
    # محرك تقييم المؤشرات والفلاتر (بشروط متوسطة)
    # ==========================================
    def check_indicators(self, asset: str, direction: str, is_pre_close: bool = False) -> tuple[bool, str]:
        if not self.indicators_enabled:
            return True, "الشطرنج فقط"

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
            if total_range <= 0:
                return False, "شمعة ساكنة بدون حركة"
            if (body / total_range) < 0.20:
                return False, f"جسم الشمعة صغير جدًا ({int((body / total_range) * 100)}%)"
            if direction == "put":
                upper_wick = h_price - max(o_price, c_price)
                if upper_wick > (2.2 * body):
                    return False, "ذيل علوي طويل معاكس للبيع"
            elif direction == "call":
                lower_wick = min(o_price, c_price) - l_price
                if lower_wick > (2.2 * body):
                    return False, "ذيل سفلي طويل معاكس للشراء"

        if self.ind_bb_enabled:
            bb = calc_bollinger(closes, period=20, mult=2.0)
            if bb is not None:
                lower, mid, upper = bb
                half_band = upper - mid
                if half_band > 0:
                    req_zone = self.ind_bb_zone if is_pre_close else max(0.45, self.ind_bb_zone - 0.25)
                    if direction == "put":
                        pos = (c_price - mid) / half_band
                        if pos < req_zone:
                            return False, f"البولينجر ({int(pos * 100)}% < {int(req_zone * 100)}%)"
                    elif direction == "call":
                        pos = (mid - c_price) / half_band
                        if pos < req_zone:
                            return False, f"البولينجر ({int(pos * 100)}% < {int(req_zone * 100)}%)"

        if self.ind_rsi_enabled:
            rsi = calc_rsi(closes, period=self.ind_rsi_period)
            if rsi is not None:
                if direction == "put":
                    req_ob = self.ind_rsi_overbought if is_pre_close else (self.ind_rsi_overbought - 6.0)
                    if rsi < req_ob:
                        return False, f"RSI ({rsi:.1f} < {req_ob:.0f})"
                elif direction == "call":
                    req_os = self.ind_rsi_oversold if is_pre_close else (self.ind_rsi_oversold + 6.0)
                    if rsi > req_os:
                        return False, f"RSI ({rsi:.1f} > {req_os:.0f})"

        if self.ind_stoch_enabled:
            stoch = calc_stochastic(ordered_candles, k_period=5, smooth=3)
            if stoch is not None:
                if direction == "put":
                    req_s_ob = self.ind_stoch_overbought if is_pre_close else (self.ind_stoch_overbought - 7.0)
                    if stoch < req_s_ob:
                        return False, f"Stoch ({stoch:.1f} < {req_s_ob:.0f})"
                elif direction == "call":
                    req_s_os = self.ind_stoch_oversold if is_pre_close else (self.ind_stoch_oversold + 7.0)
                    if stoch > req_s_os:
                        return False, f"Stoch ({stoch:.1f} > {req_s_os:.0f})"

        if self.ind_atr_enabled and is_pre_close:
            atr = calc_atr(ordered_candles, period=14)
            if atr is not None and atr > 0:
                c_range = max(h_price - l_price, abs(c_price - o_price))
                if c_range < (0.25 * atr):
                    return False, "سيولة ضعيفة جدًا (ATR)"
                if c_range > (4.0 * atr):
                    return False, "انفجار سعري مفاجئ (ATR)"

        return True, "مستوفي ✅"

    def get_active_indicators_summary(self) -> str:
        if not self.indicators_enabled:
            return "🔴 متوقف (الاعتماد على الشطرنج فقط)"
        active_list = []
        if self.ind_bb_enabled:
            active_list.append(f"BB({int(self.ind_bb_zone * 100)}%)")
        if self.ind_rsi_enabled:
            active_list.append(f"RSI({int(self.ind_rsi_oversold)}/{int(self.ind_rsi_overbought)})")
        if self.ind_wick_enabled:
            active_list.append("فلتر الذيل")
        if self.ind_stoch_enabled:
            active_list.append("Stoch")
        if self.ind_atr_enabled:
            active_list.append("ATR")
        joined = " + ".join(active_list) if active_list else "لا يوجد مؤشر محدد"
        return f"🟢 مفعّل ({joined})"

    # ==========================================
    # إدارة الحسابات المتعددة والتخزين (ديمو + حقيقي لكل حساب)
    # ==========================================
    def _normalize_account_record(self, acc: dict) -> dict:
        base_frame = acc.get("demo_auth_frame") or acc.get("live_auth_frame") or acc.get("auth_frame") or DEFAULT_AUTH_FRAME
        parsed = parse_auth_frame(base_frame) or {}
        uid = str(acc.get("uid") or parsed.get("uid") or "000000")
        demo_f = acc.get("demo_auth_frame") or build_auth_frame_with_mode(base_frame, True)
        live_f = acc.get("live_auth_frame") or build_auth_frame_with_mode(base_frame, False)
        return {
            "name": acc.get("name", f"حساب {uid}"),
            "uid": uid,
            "demo_auth_frame": demo_f,
            "live_auth_frame": live_f,
            "auth_frame": demo_f
        }

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
                    self.web_connect_token = str(data.get("web_token", "")).strip()
                    loaded = True
            except Exception as e:
                log("ACC_WARN", f"تعذر قراءة ملف الحسابات: {e}")

        if not self.web_connect_token:
            self.web_connect_token = secrets.token_urlsafe(16)

        if not loaded or not self.accounts:
            parsed = parse_auth_frame(DEFAULT_AUTH_FRAME) or {}
            uid = str(parsed.get("uid", "142271281"))
            self.is_demo_mode = bool(parsed.get("isDemo", 1) == 1)
            self.accounts = [self._normalize_account_record({
                "name": "الحساب الافتراضي",
                "uid": uid,
                "auth_frame": DEFAULT_AUTH_FRAME
            })]
            self.active_account_idx = 0
            self._save_accounts_storage()

        self._sync_active_auth_frame()

    def _save_accounts_storage(self):
        try:
            with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "active_idx": self.active_account_idx,
                    "is_demo_mode": self.is_demo_mode,
                    "web_token": self.web_connect_token,
                    "accounts": self.accounts
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log("ACC_WARN", f"تعذر حفظ ملف الحسابات: {e}")

    def _sync_active_auth_frame(self):
        if not self.accounts:
            return
        self.active_account_idx = max(0, min(self.active_account_idx, len(self.accounts) - 1))
        acc = self.accounts[self.active_account_idx]
        if self.is_demo_mode:
            self.auth_frame = acc.get("demo_auth_frame") or build_auth_frame_with_mode(acc["auth_frame"], True)
        else:
            self.auth_frame = acc.get("live_auth_frame") or build_auth_frame_with_mode(acc["auth_frame"], False)

    def get_active_account(self) -> dict:
        if not self.accounts:
            return {"name": "غير محدد", "uid": "000000", "auth_frame": self.auth_frame}
        return self.accounts[self.active_account_idx]

    def get_mobile_connect_url(self, acc_name: str = "") -> str:
        url = f"{PUBLIC_BASE_URL}/connect?token={quote(self.web_connect_token)}"
        if acc_name:
            url += f"&name={quote(acc_name)}"
        return url

    def register_captured_account(self, name: str, uid: str, demo_session: str, live_session: str) -> dict:
        """حفظ أو تحديث حساب تم ربطه من الهاتف (يشمل جلسة الديمو والحقيقي معًا)"""
        clean_uid = str(uid).strip() or "Unknown"
        clean_name = (name or "").strip()[:30] or f"حساب {clean_uid}"

        s_demo = (demo_session or live_session or "").strip()
        s_live = (live_session or demo_session or "").strip()

        demo_frame = make_auth_frame(s_demo, clean_uid, is_demo=True)
        live_frame = make_auth_frame(s_live, clean_uid, is_demo=False)

        existing_idx = None
        for idx, acc in enumerate(self.accounts):
            if str(acc.get("uid")) == clean_uid:
                existing_idx = idx
                break

        record = {
            "name": clean_name,
            "uid": clean_uid,
            "demo_auth_frame": demo_frame,
            "live_auth_frame": live_frame,
            "auth_frame": demo_frame
        }

        if existing_idx is not None:
            self.accounts[existing_idx] = record
            self.active_account_idx = existing_idx
        else:
            self.accounts.append(record)
            self.active_account_idx = len(self.accounts) - 1

        self.account_balance = None
        self.reset_boost_state(reset_peak=True)
        self._sync_active_auth_frame()
        self._save_accounts_storage()
        return record

    def reset_boost_state(self, reset_peak: bool = False):
        self.in_boost_recovery = False
        self.boost_wins_done = 0
        self.current_cycle_spent = 0.0
        self.effective_base_amount = self.base_amount
        if reset_peak:
            self.peak_balance = float(self.account_balance) if self.account_balance is not None else None

    def record_trade_outcome(self, pair_name: str, is_win: bool, attempt_num: int):
        today_str = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
        if today_str != self.stats_date:
            self.reset_daily_stats()

        self.daily_records.append({
            "pair": pair_name,
            "result": "WIN" if is_win else "LOSS",
            "step": attempt_num
        })

    def format_daily_stats_report(self) -> str:
        now_dt = datetime.now(tz=TZ_UTC3)
        days_ar = {
            "Monday": "الإثنين", "Tuesday": "الثلاثاء", "Wednesday": "الأربعاء",
            "Thursday": "الخميس", "Friday": "الجمعة", "Saturday": "السبت", "Sunday": "الأحد"
        }
        day_name = days_ar.get(now_dt.strftime("%A"), now_dt.strftime("%A"))
        date_header = f"{now_dt.strftime('%d %b %Y')} • {day_name}"

        total_signals = len(self.daily_records)
        wins = [r for r in self.daily_records if r["result"] == "WIN"]
        losses = [r for r in self.daily_records if r["result"] == "LOSS"]

        win_count = len(wins)
        loss_count = len(losses)
        win_rate = (win_count / total_signals * 100.0) if total_signals > 0 else 0.0

        sure_shot = sum(1 for r in wins if r["step"] == 1)
        mtg1 = sum(1 for r in wins if r["step"] == 2)
        mtg2 = sum(1 for r in wins if r["step"] == 3)
        mtg3_plus = sum(1 for r in wins if r["step"] >= 4)

        def pct(val: int) -> int:
            return round((val / total_signals) * 100) if total_signals > 0 else 0

        best_streak = 0
        curr_streak = 0
        for r in self.daily_records:
            if r["result"] == "WIN":
                curr_streak += 1
                if curr_streak > best_streak:
                    best_streak = curr_streak
            else:
                curr_streak = 0

        timeline_icons = []
        step_badges = {1: "✅", 2: "1️⃣", 3: "2️⃣", 4: "3️⃣", 5: "4️⃣", 6: "5️⃣"}
        for r in self.daily_records:
            if r["result"] == "WIN":
                timeline_icons.append(step_badges.get(r["step"], "✅"))
            else:
                timeline_icons.append("❌")

        if timeline_icons:
            rows = [
                " ".join(timeline_icons[i:i + 10])
                for i in range(0, len(timeline_icons), 10)
            ]
            timeline_block = "\n".join(f"┃ {row}" for row in rows)
        else:
            timeline_block = "┃ <i>لا توجد صفقات مسجلة اليوم بعد</i>"

        pair_stats = {}
        for r in self.daily_records:
            p = r["pair"]
            if p not in pair_stats:
                pair_stats[p] = {"wins": 0, "total": 0}
            pair_stats[p]["total"] += 1
            if r["result"] == "WIN":
                pair_stats[p]["wins"] += 1

        sorted_pairs = sorted(
            pair_stats.items(),
            key=lambda x: (x[1]["wins"], x[1]["wins"] / x[1]["total"]),
            reverse=True
        )[:3]

        if sorted_pairs:
            top_pairs_str = "  |  ".join(
                f"<code>{p}</code> ({d['wins']}/{d['total']})" for p, d in sorted_pairs
            )
        else:
            top_pairs_str = "لا يوجد بعد"

        mtg3_line = (
            f"┃ 🔵 <b>MTG 3+</b>    {make_progress_bar(mtg3_plus, total_signals)}  <b>{mtg3_plus}</b> ({pct(mtg3_plus)}%)\n"
            if mtg3_plus > 0 else ""
        )

        turnover_str = format_money(self.total_turnover)

        return (
            f"╭━━━[ 🛡 <b>عبود تريدينج</b> ]━━━╮\n"
            f"┃ 📊 <b>DAILY RESULTS — النتائج اليومية</b>\n"
            f"┃ 📅 <code>{date_header}</code>\n"
            f"┃ ⏱ POCKET OPTION • M1\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"┃ 🎯 <b>WIN RATE:</b> <b>{win_rate:.1f}%</b>  |  📡 <b>SIGNALS:</b> <b>{total_signals}</b>\n"
            f"┃ 🟢 <b>{win_count} WINS</b>      🔴 <b>{loss_count} LOSSES</b>\n"
            f"┃ 💵 <b>عائد التداول:</b> <b>${turnover_str}</b>\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"┃ 📋 <b>BREAKDOWN</b>   |   🔥 <b>BEST STREAK: {best_streak} WINS</b>\n"
            f"┃ 🟢 <b>SURE SHOT</b>  {make_progress_bar(sure_shot, total_signals)}  <b>{sure_shot}</b> ({pct(sure_shot)}%)\n"
            f"┃ 🟡 <b>MTG 1</b>         {make_progress_bar(mtg1, total_signals)}  <b>{mtg1}</b> ({pct(mtg1)}%)\n"
            f"┃ 🟠 <b>MTG 2</b>         {make_progress_bar(mtg2, total_signals)}  <b>{mtg2}</b> ({pct(mtg2)}%)\n"
            f"{mtg3_line}"
            f"┃ 🔴 <b>LOSS</b>           {make_progress_bar(loss_count, total_signals)}  <b>{loss_count}</b> ({pct(loss_count)}%)\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"┃ 🕒 <b>TRADE TIMELINE</b> (✅ أساسي | 1️⃣2️⃣ تدبيل | ❌ خسارة)\n"
            f"{timeline_block}\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"┃ 🏆 <b>TOP PAIRS:</b>\n"
            f"┃ {top_pairs_str}\n"
            f"╰━━━━━━━━━━━━━━━━━━━━━━━╯"
        )

    def get_scope_label(self) -> str:
        labels = {
            "CURRENCIES": "💱 العملات فقط",
            "CRYPTO": "🪙 الكريبتو فقط",
            "STOCKS": "📈 الأسهم فقط",
            "CURR_CRYPTO": "💱+🪙 العملات والكريبتو",
            "ALL": "🌐 الكل (عملات + كريبتو + أسهم)",
        }
        return labels.get(self.scan_scope, self.scan_scope)

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
            pairs = (
                self.assets_by_cat["CURRENCIES"]
                + self.assets_by_cat["CRYPTO"]
                + self.assets_by_cat["STOCKS"]
            )
        return list(dict.fromkeys(pairs))

    def format_signal_symbol(self, symbol: str) -> str:
        if symbol in self.asset_display_names and self.asset_display_names[symbol]:
            return self.asset_display_names[symbol].replace("/", "").replace(" OTC", "-OTC").strip()
        return symbol.replace("_otc", "-OTC").replace("#", "")

    def get_current_entry_base(self) -> float:
        if self.boost_enabled and self.in_boost_recovery:
            return self.effective_base_amount
        return self.base_amount

    def get_trade_ladder(self, custom_base: float | None = None) -> list[float]:
        active_base = custom_base if custom_base is not None else self.get_current_entry_base()
        steps = self.global_steps if self.martingale_enabled else 1
        ladder = []
        for i in range(steps):
            val = active_base * (2 ** i)
            ladder.append(int(val) if val.is_integer() else round(val, 2))
        return ladder

    def get_ws_url(self) -> str:
        if self.is_demo_mode:
            return "wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket"
        return "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket"

    def get_server_now(self) -> float:
        return time.time() + self.server_hour_shift

    def refresh_mode_state(self):
        if self.active_trades:
            self.mode = "IN_TRADE"
        elif self.locked_candidates:
            self.mode = "LOCKED"
        else:
            self.mode = "SCANNING"

    def enqueue_new_signal(self, text: str) -> int:
        self.signal_seq += 1
        uid = self.signal_seq
        self.channel_queue.put_nowait(("NEW", uid, text))
        return uid

    def enqueue_signal_reply(self, signal_uid: int | None, text: str):
        self.channel_queue.put_nowait(("REPLY", signal_uid, text))

    async def ensure_subscribed(self, pair: str):
        if self.ws and self.authenticated and pair not in self.subscribed_pairs:
            try:
                await self.ws.send(f'42["subfor","{pair}"]')
                self.subscribed_pairs.add(pair)
            except Exception:
                pass

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

    # ==========================================
    # واجهة التحكم (خاص بالأدمن) + إدارة الحسابات
    # ==========================================
    def build_main_keyboard(self) -> dict:
        toggle_btn = (
            {"text": "⏹ إيقاف التداول", "callback_data": "cmd_stop"}
            if self.is_running
            else {"text": "▶️ بدء التداول", "callback_data": "cmd_start"}
        )
        mode_btn = (
            {"text": "🧪 الحساب: تجريبي (Demo)", "callback_data": "toggle_account_mode"}
            if self.is_demo_mode
            else {"text": "💵 الحساب: حقيقي (Live) 🔥", "callback_data": "toggle_account_mode"}
        )
        ind_master_btn = (
            {"text": "📊 المؤشرات: مفعّل 🟢", "callback_data": "toggle_indicators_master"}
            if self.indicators_enabled
            else {"text": "📊 المؤشرات: متوقف 🔴", "callback_data": "toggle_indicators_master"}
        )
        fast_btn = (
            {"text": "⚡ الوضع السريع: مفعّل 🟢", "callback_data": "toggle_fast_mode"}
            if self.fast_mode
            else {"text": "⚡ الوضع السريع: متوقف 🔴", "callback_data": "toggle_fast_mode"}
        )
        mtg_toggle_btn = (
            {"text": "🔁 التدبيل: مفعّل 🟢", "callback_data": "toggle_martingale"}
            if self.martingale_enabled
            else {"text": "🔁 التدبيل: متوقف 🔴", "callback_data": "toggle_martingale"}
        )
        boost_btn = (
            {"text": "🛡️ التعزيز: مفعّل 🟢", "callback_data": "toggle_boost"}
            if self.boost_enabled
            else {"text": "🛡️ التعزيز: متوقف 🔴", "callback_data": "toggle_boost"}
        )
        cont_btn = (
            {"text": "🔄 مواصلة النموذج: مفعّل 🟢", "callback_data": "toggle_cont"}
            if self.continuation_enabled
            else {"text": "🔄 مواصلة النموذج: متوقف 🔴", "callback_data": "toggle_cont"}
        )

        dual_label = f"👯 المزدوج ({self.max_locked_pairs} عملات)" if self.max_locked_pairs > 1 else "👯 المزدوج (1 عملة)"
        trades_label = f"🎯 الصفقات ({self.max_concurrent_trades} من {self.max_locked_pairs})"

        return {
            "inline_keyboard": [
                [toggle_btn, {"text": "📡 تحديث الحالة", "callback_data": "cmd_status"}],
                [mode_btn, {"text": "👤 إدارة الحسابات", "callback_data": "menu_accounts"}],
                [ind_master_btn, {"text": "⚙️ ضبط المؤشرات", "callback_data": "menu_indicators"}],
                [{"text": f"🎯 مستوى المسح: {self.get_scope_label()}", "callback_data": "menu_scope"}],
                [
                    {"text": dual_label, "callback_data": "set_dual_pairs"},
                    {"text": trades_label, "callback_data": "set_concurrent_trades"}
                ],
                [
                    fast_btn,
                    cont_btn
                ],
                [
                    {"text": f"💵 الأساسي (${self.base_amount:g})", "callback_data": "set_amount"},
                    {"text": f"📈 العائد (≥ {self.min_payout}%)", "callback_data": "set_payout"}
                ],
                [
                    mtg_toggle_btn,
                    {"text": f"🌐 التدبيل العام ({self.global_steps})", "callback_data": "set_global_steps"}
                ],
                [
                    {"text": f"🏛 تدبيل الزوج ({self.pair_steps})", "callback_data": "set_pair_steps"},
                    {"text": f"⏱ التقديم ({self.entry_lead_seconds}ث)", "callback_data": "set_lead"}
                ],
                [
                    boost_btn,
                    {"text": "⚙️ ضبط التعزيز", "callback_data": "menu_boost"}
                ],
                [
                    {"text": "📊 معاينة الإحصائيات", "callback_data": "stats_preview"},
                    {"text": "📢 نشر بالقناة", "callback_data": "stats_publish"}
                ],
                [
                    {"text": "🗑️ تصفير الإحصائيات", "callback_data": "stats_reset"}
                ]
            ]
        }

    def build_boost_keyboard(self) -> dict:
        en_btn = (
            {"text": "🛡️ حالة التعزيز: مفعّل 🟢", "callback_data": "boost_toggle_enable"}
            if self.boost_enabled
            else {"text": "🛡️ حالة التعزيز: متوقف 🔴", "callback_data": "boost_toggle_enable"}
        )
        mode_btn = (
            {"text": "🎯 نوع التعزيز: تعويض القمة 💰", "callback_data": "boost_toggle_mode"}
            if self.boost_mode == "CAPITAL"
            else {"text": "🎯 نوع التعزيز: عدد صفقات ناجحة 🏆", "callback_data": "boost_toggle_mode"}
        )

        return {
            "inline_keyboard": [
                [en_btn],
                [mode_btn],
                [
                    {"text": f"➕ زيادة التعزيز (+${self.boost_increment:g})", "callback_data": "set_boost_inc"},
                    {"text": f"🔢 عدد الصفقات ({self.boost_target_wins})", "callback_data": "set_boost_wins"}
                ],
                [
                    {"text": "🔄 تصفير التعزيز والعودة للأساسي", "callback_data": "boost_reset_now"}
                ],
                [
                    {"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}
                ]
            ]
        }

    def format_boost_menu_text(self) -> str:
        en_str = "🟢 <b>مفعّل</b>" if self.boost_enabled else "🔴 <b>متوقف</b>"
        mode_str = (
            "💰 <b>تعويض رأس المال إلى القمة</b> (يستمر بالتعزيز حتى يعود الرصيد لأعلى قمة وصل إليها)"
            if self.boost_mode == "CAPITAL"
            else f"🏆 <b>عدد صفقات ناجحة محددة</b> (يستمر بالتعزيز حتى يحقق <b>{self.boost_target_wins}</b> صفقات رابحة)"
        )
        peak_str = f"${format_money(self.peak_balance)}" if self.peak_balance is not None else "غير مسجل بعد"
        bal_str = f"${format_money(self.account_balance)}" if self.account_balance is not None else "غير متاح"

        if self.in_boost_recovery:
            if self.boost_mode == "CAPITAL":
                prog = f"🔥 <b>نشط الآن (للصفقات الجديدة)</b> | الأساسي المعزّز: <b>${self.effective_base_amount:g}</b> | الرصيد: <code>{bal_str}</code> / الهدف: <code>{peak_str}</code>"
            else:
                prog = f"🔥 <b>نشط الآن (للصفقات الجديدة)</b> | الأساسي المعزّز: <b>${self.effective_base_amount:g}</b> | المنجز: <code>{self.boost_wins_done}/{self.boost_target_wins}</code> صفقات"
        else:
            prog = f"⏳ في وضع الاستعداد (الأساسي الحالي: <b>${self.base_amount:g}</b>)"

        return (
            f"🛡️ <b>إعدادات التعزيز الذكي بعد الخسارة</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>الحالة:</b> {en_str}\n"
            f"🎯 <b>نظام التعزيز:</b> {mode_str}\n"
            f"➕ <b>مقدار الزيادة الواحدة:</b> <code>+${self.boost_increment:g}</code> (تبدأ من الصفقات الجديدة فقط)\n"
            f"🏆 <b>عدد الصفقات المستهدفة:</b> <code>{self.boost_target_wins} صفقات ناجحة</code>\n"
            f"🏔 <b>قمة رأس المال المسجلة:</b> <code>{peak_str}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📍 <b>الوضع الحالي:</b> {prog}"
        )

    def build_indicators_keyboard(self) -> dict:
        master_btn = (
            {"text": "🎛 التفعيل العام للمؤشرات: مفعّل 🟢", "callback_data": "ind_toggle_master"}
            if self.indicators_enabled
            else {"text": "🎛 التفعيل العام للمؤشرات: متوقف 🔴", "callback_data": "ind_toggle_master"}
        )
        bb_btn = (
            {"text": f"📈 البولينجر باند ({int(self.ind_bb_zone * 100)}%): 🟢", "callback_data": "ind_toggle_bb"}
            if self.ind_bb_enabled
            else {"text": "📈 البولينجر باند: 🔴", "callback_data": "ind_toggle_bb"}
        )
        rsi_btn = (
            {"text": f"📉 RSI ({int(self.ind_rsi_oversold)}/{int(self.ind_rsi_overbought)}): 🟢", "callback_data": "ind_toggle_rsi"}
            if self.ind_rsi_enabled
            else {"text": "📉 مؤشر RSI: 🔴", "callback_data": "ind_toggle_rsi"}
        )
        wick_btn = (
            {"text": "🕯️ فلتر الجسم والذيل: 🟢", "callback_data": "ind_toggle_wick"}
            if self.ind_wick_enabled
            else {"text": "🕯️ فلتر الجسم والذيل: 🔴", "callback_data": "ind_toggle_wick"}
        )
        stoch_btn = (
            {"text": f"⚡ ستوكاستيك ({int(self.ind_stoch_oversold)}/{int(self.ind_stoch_overbought)}): 🟢", "callback_data": "ind_toggle_stoch"}
            if self.ind_stoch_enabled
            else {"text": "⚡ ستوكاستيك: 🔴", "callback_data": "ind_toggle_stoch"}
        )
        atr_btn = (
            {"text": "🌊 فلتر السيولة ATR: 🟢", "callback_data": "ind_toggle_atr"}
            if self.ind_atr_enabled
            else {"text": "🌊 فلتر السيولة ATR: 🔴", "callback_data": "ind_toggle_atr"}
        )

        return {
            "inline_keyboard": [
                [master_btn],
                [bb_btn, {"text": "🎚 حساسية BB", "callback_data": "ind_set_bb"}],
                [rsi_btn, {"text": "🎚 مستويات RSI", "callback_data": "ind_set_rsi"}],
                [wick_btn, stoch_btn],
                [atr_btn],
                [{"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}]
            ]
        }

    def format_indicators_menu_text(self) -> str:
        master_str = "🟢 <b>مفعّل</b> (يتم دمج المؤشرات مع الشطرنج)" if self.indicators_enabled else "🔴 <b>متوقف</b> (الاعتماد على الشطرنج فقط)"
        return (
            f"📊 <b>إعدادات المؤشرات والفلاتر المساعدة</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎛 <b>الحالة العامة:</b> {master_str}\n\n"
            f"• <b>📈 البولينجر باند (20, 2):</b> {'🟢 مفعّل' if self.ind_bb_enabled else '🔴 متوقف'} "
            f"(نطاق الوصول: <code>{int(self.ind_bb_zone * 100)}%</code>)\n"
            f"• <b>📉 مؤشر RSI (14):</b> {'🟢 مفعّل' if self.ind_rsi_enabled else '🔴 متوقف'} "
            f"(شراء ≤ <code>{int(self.ind_rsi_oversold)}</code> | بيع ≥ <code>{int(self.ind_rsi_overbought)}</code>)\n"
            f"• <b>🕯️ فلتر الجسم والذيل:</b> {'🟢 مفعّل' if self.ind_wick_enabled else '🔴 متوقف'} "
            f"(يمنع الدوجي والذيل العكسي الطويل)\n"
            f"• <b>⚡ ستوكاستيك (5,3,3):</b> {'🟢 مفعّل' if self.ind_stoch_enabled else '🔴 متوقف'} "
            f"(شراء ≤ <code>{int(self.ind_stoch_oversold)}</code> | بيع ≥ <code>{int(self.ind_stoch_overbought)}</code>)\n"
            f"• <b>🌊 فلتر السيولة ATR (14):</b> {'🟢 مفعّل' if self.ind_atr_enabled else '🔴 متوقف'}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<i>ملاحظة: الشروط مضبوطة بدرجة متوسطة لرفع دقة الإشارات دون تفويت الفرص.</i>"
        )

    def build_accounts_keyboard(self) -> dict:
        rows = []
        for idx, acc in enumerate(self.accounts):
            prefix = "✅ " if idx == self.active_account_idx else "👤 "
            rows.append([{
                "text": f"{prefix}{acc['name']} ({acc['uid']}) [Demo+Live]",
                "callback_data": f"select_acc_{idx}"
            }])

        rows.append([{
            "text": "📱 فتح بوابة الربط من الهاتف",
            "url": self.get_mobile_connect_url()
        }])
        rows.append([{
            "text": "📲 إرسال رابط الربط للنسخ (للمتصفح الخارجي)",
            "callback_data": "send_mobile_link_copy"
        }])
        rows.append([
            {"text": "⌨️ إضافة يدوية بالجلسة", "callback_data": "add_account_start"},
            {"text": "🗑️ حذف حساب", "callback_data": "menu_delete_account"}
        ])
        rows.append([
            {"text": "🔑 تحديث جلسة الحساب الحالي", "callback_data": "set_auth"}
        ])
        rows.append([
            {"text": "🔙 رجوع للقائمة الرئيسية", "callback_data": "cmd_status"}
        ])
        return {"inline_keyboard": rows}

    def build_delete_accounts_keyboard(self) -> dict:
        rows = []
        for idx, acc in enumerate(self.accounts):
            rows.append([{
                "text": f"🗑️ حذف: {acc['name']} ({acc['uid']})",
                "callback_data": f"del_acc_{idx}"
            }])
        rows.append([{"text": "🔙 رجوع لإدارة الحسابات", "callback_data": "menu_accounts"}])
        return {"inline_keyboard": rows}

    def build_scope_keyboard(self) -> dict:
        def mark(code: str, title: str) -> str:
            return f"✅ {title}" if self.scan_scope == code else title

        return {
            "inline_keyboard": [
                [{"text": mark("CURRENCIES", "💱 أزواج العملات فقط"), "callback_data": "scope_CURRENCIES"}],
                [{"text": mark("CRYPTO", "🪙 أزواج الكريبتو فقط"), "callback_data": "scope_CRYPTO"}],
                [{"text": mark("STOCKS", "📈 الأسهم فقط"), "callback_data": "scope_STOCKS"}],
                [{"text": mark("CURR_CRYPTO", "💱 + 🪙 العملات والكريبتو معًا"), "callback_data": "scope_CURR_CRYPTO"}],
                [{"text": mark("ALL", "🌐 جميع الأسواق (عملات + كريبتو + أسهم)"), "callback_data": "scope_ALL"}],
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
        bal_label = "رصيد الديمو" if self.is_demo_mode else "الرصيد الحقيقي"
        bal_str = f"${format_money(self.account_balance)}" if self.account_balance is not None else "جاري الجلب..."

        active_pairs = self.get_active_scan_pairs()
        eligible_count = sum(1 for p in active_pairs if self.payouts.get(p, 0) >= self.min_payout)

        total_today = len(self.daily_records)
        wins_today = sum(1 for r in self.daily_records if r["result"] == "WIN")
        rate_today = (wins_today / total_today * 100.0) if total_today > 0 else 0.0
        turnover_str = format_money(self.total_turnover)

        fast_status = "🟢 مفعّل (مسح مستمر وتجهيز متوازي)" if self.fast_mode else "🔴 متوقف"
        mtg_status = (
            f"🟢 مفعّل (<code>{ladder_str}</code>)"
            if self.martingale_enabled
            else f"🔴 متوقف (دخول ثابت: <code>${current_next_amt}</code>)"
        )

        if self.boost_enabled:
            if self.boost_mode == "CAPITAL":
                peak_txt = f"${format_money(self.peak_balance)}" if self.peak_balance is not None else "—"
                if self.in_boost_recovery:
                    boost_status = f"🟢 مفعّل [تعويض القمة] (للجديدة: <b>${self.effective_base_amount:g}</b> | الهدف: <code>{peak_txt}</code>)"
                else:
                    boost_status = f"🟢 مفعّل [تعويض القمة] (+${self.boost_increment:g} | القمة: <code>{peak_txt}</code>)"
            else:
                if self.in_boost_recovery:
                    boost_status = f"🟢 مفعّل [عدد صفقات] (للجديدة: <b>${self.effective_base_amount:g}</b> | المنجز: <code>{self.boost_wins_done}/{self.boost_target_wins}</code>)"
                else:
                    boost_status = f"🟢 مفعّل [عدد صفقات] (+${self.boost_increment:g} | الهدف: <code>{self.boost_target_wins} صفقات</code>)"
        else:
            boost_status = "🔴 متوقف"

        cont_status = "🟢 مفعّل (عكس الصفقة بعد WIN¹)" if self.continuation_enabled else "🔴 متوقف"

        activity_parts = []
        if self.active_trades:
            t_names = " | ".join(
                f"{self.format_signal_symbol(t['asset'])} (${t['amount']})" for t in self.active_trades
            )
            activity_parts.append(f"⚡ نشط ({len(self.active_trades)}): {t_names}")
        if self.locked_candidates:
            cand_names = " ⬅️ ".join(
                f"#{c['rank']} {self.format_signal_symbol(c['asset'])}" for c in self.locked_candidates
            )
            activity_parts.append(f"🎯 مجهّز ({len(self.locked_candidates)}/{self.max_locked_pairs}): {cand_names}")
        if not activity_parts:
            activity_parts.append(f"🔍 يبحث عن فرصة (الدخول القادم: ${current_next_amt})")
        mode_ar = " + ".join(activity_parts)

        return (
            f"♟ <b>لوحة إدارة عبود تريدينج (24/7)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>الحالة:</b> {status_icon}\n"
            f"👤 <b>الحساب النشط:</b> <b>{acc['name']}</b> (<code>{acc['uid']}</code>)\n"
            f"🎛 <b>نوع الحساب:</b> {acc_mode_str}\n"
            f"💰 <b>{bal_label}:</b> <code>{bal_str}</code>\n"
            f"🌐 <b>السيرفر:</b> {conn_icon}\n"
            f"📍 <b>النشاط:</b> {mode_ar}\n"
            f"📊 <b>إحصائيات اليوم:</b> <code>{wins_today}/{total_today}</code> ({rate_today:.1f}%)\n"
            f"💵 <b>عائد التداول (Turnover):</b> <code>${turnover_str}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>مستوى المسح:</b> <b>{self.get_scope_label()}</b>\n"
            f"📊 <b>المؤشرات والفلاتر:</b> {self.get_active_indicators_summary()}\n"
            f"👯 <b>المزدوج / الصفقات:</b> يمسك <code>{self.max_locked_pairs}</code> عملات | يدخل <code>{self.max_concurrent_trades}</code> صفقات\n"
            f"⚡ <b>الوضع السريع:</b> {fast_status}\n"
            f"🔄 <b>مواصلة النموذج:</b> {cont_status}\n"
            f"📈 <b>الأصول المؤهلة (≥{self.min_payout}%):</b> <code>{eligible_count}</code> من {len(active_pairs)}\n"
            f"💵 <b>المبلغ الأساسي:</b> <code>${self.base_amount:g}</code>\n"
            f"🔁 <b>حالة التدبيل:</b> {mtg_status}\n"
            f"🏛 <b>تدبيل الزوج الواحد:</b> <code>{self.pair_steps} محاولات كحد أقصى</code>\n"
            f"🛡️ <b>وضع التعزيز:</b> {boost_status}\n"
            f"⏱ <b>التقديم قبل الإغلاق:</b> <code>{self.entry_lead_seconds} ثانية</code>\n"
            f"📢 <b>قناة الإشارات:</b> <code>{CHANNEL_ID}</code>"
        )

    async def tg_api(self, method: str, payload: dict, req_timeout: float = 15.0, retries: int = 3):
        if not self.http_session:
            return None
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
        timeout_cfg = aiohttp.ClientTimeout(total=req_timeout)

        for attempt in range(1, retries + 1):
            try:
                async with self.http_session.post(url, json=payload, timeout=timeout_cfg) as resp:
                    return await resp.json()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt == retries:
                    log("TG_WARN", f"تعذر الوصول لتليجرام ({method}): {type(e).__name__}")
                    return None
                await asyncio.sleep(0.5 * attempt)
        return None

    async def send_admin(self, text: str, reply_markup: dict = None):
        payload = {"chat_id": ADMIN_CHAT_ID, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        await self.tg_api("sendMessage", payload, req_timeout=15.0, retries=3)

    async def send_channel(self, text: str, reply_to_message_id: int = None) -> int | None:
        if not CHANNEL_ID or "ضع_" in str(CHANNEL_ID):
            log("WARN", "لم يتم ضبط CHANNEL_ID بعد — تعذّر الإرسال للقناة.")
            return None
        payload = {
            "chat_id": CHANNEL_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
            payload["allow_sending_without_reply"] = True

        res = await self.tg_api("sendMessage", payload, req_timeout=15.0, retries=3)
        if res and res.get("ok"):
            msg_id = res.get("result", {}).get("message_id")
            log("CHANNEL", f"تم نشر الرسالة في القناة بنجاح ✅ (msg_id={msg_id}, reply_to={reply_to_message_id})")
            return msg_id
        else:
            log("CHANNEL_ERR", f"فشل النشر في القناة: {res}")
            return None

    async def channel_sender_worker(self):
        while True:
            kind, uid, text = await self.channel_queue.get()
            try:
                if kind == "NEW":
                    msg_id = await self.send_channel(text, reply_to_message_id=None)
                    if uid is not None and msg_id is not None:
                        self.signal_msg_ids[uid] = msg_id
                        if len(self.signal_msg_ids) > 150:
                            oldest_uid = min(self.signal_msg_ids.keys())
                            del self.signal_msg_ids[oldest_uid]
                elif kind == "REPLY":
                    parent_msg_id = self.signal_msg_ids.get(uid) if uid is not None else None
                    await self.send_channel(text, reply_to_message_id=parent_msg_id)
            except Exception as e:
                log("CH_WORKER_ERR", f"خطأ في طابور القناة: {e}")
            finally:
                self.channel_queue.task_done()

    async def daily_midnight_stats_scheduler(self):
        while True:
            await asyncio.sleep(20)
            now_dt = datetime.now(tz=TZ_UTC3)
            today_str = now_dt.strftime("%Y-%m-%d")
            if today_str != self.stats_date:
                if self.daily_records or self.total_turnover > 0:
                    report = self.format_daily_stats_report()
                    await self.send_channel(report)
                    await self.send_admin("📊 تم نشر التقرير اليومي تلقائيًا في القناة وتصفير عداد اليوم الجديد ✅")
                self.reset_daily_stats()

    async def telegram_polling_loop(self):
        offset = 0
        log("TG", "بوت الإدارة جاهز ✅ أرسل /start في الخاص لفتح لوحة التحكم.")
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
                        chat_id = str(cq["message"]["chat"]["id"])
                        msg_id = cq["message"]["message_id"]
                        data = cq.get("data", "")

                        asyncio.create_task(self.tg_api("answerCallbackQuery", {"callback_query_id": cq["id"]}, req_timeout=10.0, retries=1))
                        if chat_id != str(ADMIN_CHAT_ID):
                            continue

                        if data == "cmd_start":
                            self.is_running = True
                            self.awaiting_input = None
                            log("CTRL", "تم تشغيل البوت من لوحة الإدارة ▶️")
                        elif data == "cmd_stop":
                            self.is_running = False
                            self.awaiting_input = None
                            self.full_reset_to_scanning("إيقاف من لوحة الإدارة")
                            log("CTRL", "تم إيقاف البوت من لوحة الإدارة ⏹")
                        elif data == "cmd_status":
                            self.awaiting_input = None
                        elif data == "send_mobile_link_copy":
                            link = self.get_mobile_connect_url()
                            await self.send_admin(
                                f"📲 <b>رابط الربط المباشر من متصفح الهاتف (Chrome / Safari):</b>\n\n"
                                f"<code>{link}</code>\n\n"
                                f"📌 <b>الخطوات السريعة:</b>\n"
                                f"1️⃣ شغّل الـ <b>VPN</b> على هاتفك.\n"
                                f"2️⃣ انسخ الرابط أعلاه وافتحه في متصفح <b>Chrome</b> أو <b>Safari</b> الخارجي.\n"
                                f"3️⃣ اتبع الخطوة الموضحة في الصفحة لربط <b>الديمو + الحقيقي + الآيدي</b> دفعة واحدة."
                            )
                            continue
                        elif data == "menu_boost":
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": self.format_boost_menu_text(),
                                "parse_mode": "HTML",
                                "reply_markup": self.build_boost_keyboard()
                            })
                            continue
                        elif data == "boost_toggle_enable":
                            self.boost_enabled = not self.boost_enabled
                            if self.boost_enabled and self.account_balance is not None and self.peak_balance is None:
                                self.peak_balance = float(self.account_balance)
                            if not self.boost_enabled:
                                self.reset_boost_state(reset_peak=False)
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": self.format_boost_menu_text(),
                                "parse_mode": "HTML",
                                "reply_markup": self.build_boost_keyboard()
                            })
                            continue
                        elif data == "boost_toggle_mode":
                            self.boost_mode = "TRADES" if self.boost_mode == "CAPITAL" else "CAPITAL"
                            if self.boost_mode == "CAPITAL" and self.account_balance is not None and self.peak_balance is None:
                                self.peak_balance = float(self.account_balance)
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": self.format_boost_menu_text(),
                                "parse_mode": "HTML",
                                "reply_markup": self.build_boost_keyboard()
                            })
                            continue
                        elif data == "boost_reset_now":
                            self.reset_boost_state(reset_peak=True)
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": self.format_boost_menu_text(),
                                "parse_mode": "HTML",
                                "reply_markup": self.build_boost_keyboard()
                            })
                            continue
                        elif data == "set_boost_wins":
                            self.awaiting_input = "BOOST_WINS"
                            await self.send_admin(
                                "🏆 أرسل الآن <b>عدد الصفقات الناجحة المطلوبة</b> في وضع التعزيز قبل العودة للمبلغ الأساسي (مثال: <code>5</code>):"
                            )
                            continue
                        elif data == "toggle_indicators_master":
                            self.indicators_enabled = not self.indicators_enabled
                            log("IND_MASTER", f"حالة المؤشرات والفلاتر: {'مفعّل 🟢' if self.indicators_enabled else 'متوقف 🔴'}")
                        elif data == "menu_indicators":
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": self.format_indicators_menu_text(),
                                "parse_mode": "HTML",
                                "reply_markup": self.build_indicators_keyboard()
                            })
                            continue
                        elif data.startswith("ind_toggle_"):
                            target = data.replace("ind_toggle_", "")
                            if target == "master":
                                self.indicators_enabled = not self.indicators_enabled
                            elif target == "bb":
                                self.ind_bb_enabled = not self.ind_bb_enabled
                            elif target == "rsi":
                                self.ind_rsi_enabled = not self.ind_rsi_enabled
                            elif target == "wick":
                                self.ind_wick_enabled = not self.ind_wick_enabled
                            elif target == "stoch":
                                self.ind_stoch_enabled = not self.ind_stoch_enabled
                            elif target == "atr":
                                self.ind_atr_enabled = not self.ind_atr_enabled

                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": self.format_indicators_menu_text(),
                                "parse_mode": "HTML",
                                "reply_markup": self.build_indicators_keyboard()
                            })
                            continue
                        elif data == "ind_set_bb":
                            self.awaiting_input = "IND_BB_ZONE"
                            await self.send_admin(
                                "📈 <b>ضبط حساسية البولينجر باند (من 50% إلى 100%):</b>\n"
                                "• <code>70</code> = متوسط ومتوازن (موصى به)\n"
                                "• <code>90</code> = صارم (عند ملامسة الخط الخارجي تمامًا)\n"
                                "أرسل الآن النسبة المطلوبة:"
                            )
                            continue
                        elif data == "ind_set_rsi":
                            self.awaiting_input = "IND_RSI_LEVELS"
                            await self.send_admin(
                                "📉 <b>ضبط مستويات مؤشر RSI (الشراء / البيع):</b>\n"
                                "أرسل الرقمين مفصولين بمسافة أو شرطة (مثال: <code>38 62</code> للمتوسط، أو <code>30 70</code> للصارم):"
                            )
                            continue
                        elif data == "toggle_fast_mode":
                            self.fast_mode = not self.fast_mode
                            log("FAST_MODE", f"حالة الوضع السريع: {'مفعّل 🟢' if self.fast_mode else 'متوقف 🔴'}")
                        elif data == "toggle_martingale":
                            self.martingale_enabled = not self.martingale_enabled
                            if not self.martingale_enabled:
                                self.global_step_index = 0
                                self.pending_martingale_steps.clear()
                            log("MTG_TOGGLE", f"حالة التدبيل: {'مفعّل 🟢' if self.martingale_enabled else 'متوقف 🔴'}")
                        elif data == "set_dual_pairs":
                            self.awaiting_input = "DUAL_PAIRS"
                            await self.send_admin(
                                "👯 <b>ضبط عدد العملات الممسوكة (المزدوج):</b>\n"
                                "أرسل الآن عدد العملات التي يمسكها البوت لمراقبتها معًا قبل إغلاق الدقيقة (مثال: <code>3</code>):"
                            )
                            continue
                        elif data == "set_concurrent_trades":
                            self.awaiting_input = "CONCURRENT_TRADES"
                            await self.send_admin(
                                f"🎯 <b>ضبط عدد الصفقات المتزامنة:</b>\n"
                                f"أرسل الآن عدد الصفقات التي يدخلها البوت في نفس الوقت من بين العملات الممسوكة "
                                f"(يلزم أن يكون الرقم <b>أقل أو يساوي {self.max_locked_pairs}</b>):"
                            )
                            continue
                        elif data == "stats_reset":
                            self.reset_daily_stats()
                            await self.send_admin("🗑️ <b>تم تصفير الإحصائيات وعائد التداول بنجاح!</b>")
                        elif data == "toggle_account_mode":
                            if self.is_demo_mode:
                                acc = self.get_active_account()
                                await self.tg_api("editMessageText", {
                                    "chat_id": ADMIN_CHAT_ID,
                                    "message_id": msg_id,
                                    "text": (
                                        f"⚠️ <b>تنبيه أمان — التحويل للحساب الحقيقي (Live)</b>\n"
                                        f"━━━━━━━━━━━━━━━━━━━━\n"
                                        f"👤 <b>الحساب:</b> {acc['name']} (<code>{acc['uid']}</code>)\n"
                                        f"💵 <b>المبلغ الأساسي الحالي:</b> <code>${self.base_amount:g}</code>\n\n"
                                        f"هل أنت متأكد من تفعيل التداول على <b>الحساب الحقيقي</b>؟"
                                    ),
                                    "parse_mode": "HTML",
                                    "reply_markup": {
                                        "inline_keyboard": [
                                            [{"text": "🔥 نعم، تفعيل الحساب الحقيقي (Live)", "callback_data": "confirm_live_mode"}],
                                            [{"text": "❌ إلغاء والبقاء على التجريبي", "callback_data": "cmd_status"}]
                                        ]
                                    }
                                })
                                continue
                            else:
                                self.is_demo_mode = True
                                self.account_balance = None
                                self.reset_boost_state(reset_peak=True)
                                self._sync_active_auth_frame()
                                self._save_accounts_storage()
                                self.full_reset_to_scanning("التحويل إلى الحساب التجريبي")
                                log("ACC_MODE", "تم التحويل إلى الحساب التجريبي (Demo) 🧪")
                                if self.ws:
                                    await self.ws.close()
                        elif data == "confirm_live_mode":
                            self.is_demo_mode = False
                            self.account_balance = None
                            self.reset_boost_state(reset_peak=True)
                            self._sync_active_auth_frame()
                            self._save_accounts_storage()
                            self.full_reset_to_scanning("التحويل إلى الحساب الحقيقي")
                            log("ACC_MODE", "🔥 تم التحويل إلى الحساب الحقيقي (Live)")
                            if self.ws:
                                await self.ws.close()
                        elif data == "menu_accounts":
                            acc = self.get_active_account()
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": (
                                    f"👤 <b>إدارة حسابات Pocket Option</b>\n"
                                    f"━━━━━━━━━━━━━━━━━━━━\n"
                                    f"🔹 <b>الحساب النشط حاليًا:</b> {acc['name']} (<code>{acc['uid']}</code>)\n"
                                    f"🔹 <b>عدد الحسابات المحفوظة:</b> <code>{len(self.accounts)}</code>\n"
                                    f"🔹 <b>ربط الهاتف:</b> شغّل الـ VPN وافتح بوابة الربط في متصفح Chrome/Safari الخارجي 📱\n\n"
                                    f"اختر حسابًا للتبديل إليه فورًا أو أضف حسابًا جديدًا:"
                                ),
                                "parse_mode": "HTML",
                                "reply_markup": self.build_accounts_keyboard()
                            })
                            continue
                        elif data.startswith("select_acc_"):
                            try:
                                idx = int(data.replace("select_acc_", ""))
                                if 0 <= idx < len(self.accounts):
                                    self.active_account_idx = idx
                                    self.account_balance = None
                                    self.reset_boost_state(reset_peak=True)
                                    self._sync_active_auth_frame()
                                    self._save_accounts_storage()
                                    self.full_reset_to_scanning("تغيير الحساب النشط")
                                    acc = self.get_active_account()
                                    log("ACC_SWITCH", f"تم التبديل إلى الحساب: {acc['name']} ({acc['uid']})")
                                    if self.ws:
                                        await self.ws.close()
                            except ValueError:
                                pass
                        elif data == "add_account_start":
                            self.awaiting_input = "ADD_ACC_NAME"
                            await self.send_admin(
                                "👤 <b>إضافة حساب يدويًا — الخطوة 1 من 2:</b>\n"
                                "أرسل الآن <b>اسمًا مميزًا</b> لهذا الحساب (مثال: <code>حسابي الرئيسي</code>):"
                            )
                            continue
                        elif data == "menu_delete_account":
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": "🗑️ <b>اختر الحساب الذي تريد حذفه:</b>",
                                "parse_mode": "HTML",
                                "reply_markup": self.build_delete_accounts_keyboard()
                            })
                            continue
                        elif data.startswith("del_acc_"):
                            try:
                                idx = int(data.replace("del_acc_", ""))
                                if len(self.accounts) <= 1:
                                    await self.send_admin("⚠️ لا يمكن حذف الحساب الوحيد المتبقي. أضف حسابًا آخر أولاً ثم احذف هذا الحساب.")
                                elif 0 <= idx < len(self.accounts):
                                    removed = self.accounts.pop(idx)
                                    if self.active_account_idx >= len(self.accounts):
                                        self.active_account_idx = 0
                                    self.account_balance = None
                                    self.reset_boost_state(reset_peak=True)
                                    self._sync_active_auth_frame()
                                    self._save_accounts_storage()
                                    self.full_reset_to_scanning("حذف حساب")
                                    log("ACC_DEL", f"تم حذف الحساب: {removed['name']} ({removed['uid']})")
                                    if self.ws:
                                        await self.ws.close()
                            except ValueError:
                                pass
                            acc = self.get_active_account()
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": (
                                    f"👤 <b>إدارة حسابات Pocket Option</b>\n"
                                    f"━━━━━━━━━━━━━━━━━━━━\n"
                                    f"🔹 <b>الحساب النشط حاليًا:</b> {acc['name']} (<code>{acc['uid']}</code>)\n"
                                    f"🔹 <b>عدد الحسابات المحفوظة:</b> <code>{len(self.accounts)}</code>"
                                ),
                                "parse_mode": "HTML",
                                "reply_markup": self.build_accounts_keyboard()
                            })
                            continue
                        elif data == "toggle_boost":
                            self.boost_enabled = not self.boost_enabled
                            if self.boost_enabled and self.account_balance is not None and self.peak_balance is None:
                                self.peak_balance = float(self.account_balance)
                            if not self.boost_enabled:
                                self.reset_boost_state(reset_peak=False)
                            log("BOOST", f"حالة وضع التعزيز: {'مفعّل 🟢' if self.boost_enabled else 'متوقف 🔴'}")
                        elif data == "toggle_cont":
                            self.continuation_enabled = not self.continuation_enabled
                            log("CONT", f"حالة مواصلة النموذج: {'مفعّل 🟢' if self.continuation_enabled else 'متوقف 🔴'}")
                        elif data == "stats_preview":
                            await self.send_admin(self.format_daily_stats_report())
                            continue
                        elif data == "stats_publish":
                            report = self.format_daily_stats_report()
                            await self.send_channel(report)
                            await self.send_admin("✅ تم نشر تقرير الإحصائيات اليومية في القناة بنجاح!")
                            continue
                        elif data == "menu_scope":
                            await self.tg_api("editMessageText", {
                                "chat_id": ADMIN_CHAT_ID,
                                "message_id": msg_id,
                                "text": "🎯 <b>اختر مستوى ونطاق المسح المطلوب:</b>",
                                "parse_mode": "HTML",
                                "reply_markup": self.build_scope_keyboard()
                            })
                            continue
                        elif data.startswith("scope_"):
                            self.scan_scope = data.replace("scope_", "")
                            log("SCOPE", f"تم تغيير مستوى المسح إلى: {self.get_scope_label()}")
                            if self.mode in ("SCANNING", "LOCKED"):
                                self.switch_pair_keep_global("تغيير مستوى المسح")
                        elif data == "set_amount":
                            self.awaiting_input = "BASE_AMOUNT"
                            await self.send_admin("💵 أرسل الآن <b>المبلغ الأساسي</b> للصفقة (مثال: <code>10</code>):")
                            continue
                        elif data == "set_boost_inc":
                            self.awaiting_input = "BOOST_INC"
                            await self.send_admin(
                                "🛡️ أرسل الآن <b>مقدار الزيادة الواحدة في التعزيز</b> التي تُضاف على المبلغ الأساسي للصفقات الجديدة عند الخسارة "
                                "(مثال: <code>2</code> أو <code>10</code>):"
                            )
                            continue
                        elif data == "set_global_steps":
                            self.awaiting_input = "GLOBAL_STEPS"
                            await self.send_admin("🌐 أرسل الآن <b>عدد التدبيلات العام</b> الإجمالي (مثال: <code>6</code>):")
                            continue
                        elif data == "set_pair_steps":
                            self.awaiting_input = "PAIR_STEPS"
                            await self.send_admin("🏛 أرسل الآن <b>عدد التدبيلات على مستوى الزوج الواحد</b> (مثال: <code>4</code>):")
                            continue
                        elif data == "set_payout":
                            self.awaiting_input = "PAYOUT"
                            await self.send_admin("📈 أرسل الآن <b>الحد الأدنى لنسبة العائد %</b> (مثال: <code>90</code>):")
                            continue
                        elif data == "set_lead":
                            self.awaiting_input = "LEAD_TIME"
                            await self.send_admin("⏱ أرسل الآن <b>الفحص والدخول قبل الإغلاق بأجزاء الثانية</b> (مثال: <code>0.1</code>):")
                            continue
                        elif data == "set_auth":
                            self.awaiting_input = "AUTH"
                            acc = self.get_active_account()
                            await self.send_admin(
                                f'🔑 أرسل الآن كود الجلسة الجديد (<code>42["auth",...]</code>) للحساب <b>{acc["name"]}</b>:'
                            )
                            continue

                        await self.tg_api("editMessageText", {
                            "chat_id": ADMIN_CHAT_ID,
                            "message_id": msg_id,
                            "text": self.format_dashboard_text(),
                            "parse_mode": "HTML",
                            "reply_markup": self.build_main_keyboard()
                        })

                    elif "message" in update:
                        msg = update["message"]
                        chat_id = str(msg["chat"]["id"])
                        if chat_id != str(ADMIN_CHAT_ID):
                            continue

                        fwd_chat = msg.get("forward_from_chat")
                        if fwd_chat and fwd_chat.get("type") == "channel":
                            ch_id = fwd_chat.get("id")
                            ch_title = fwd_chat.get("title", "القناة")
                            await self.send_admin(
                                f"🆔 <b>تم استخراج آيدي القناة:</b>\n"
                                f"📢 <b>القناة:</b> {ch_title}\n"
                                f"🔢 <b>الآيدي:</b> <code>{ch_id}</code>"
                            )
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

                        if text in ("/reset_stats", "تصفير الاحصائيات", "تصفير الإحصائيات"):
                            self.reset_daily_stats()
                            await self.send_admin(
                                "🗑️ <b>تم تصفير الإحصائيات وعائد التداول بنجاح!</b>",
                                self.build_main_keyboard()
                            )
                            continue

                        if text in ("/reset_boost", "تصفير التعزيز"):
                            self.reset_boost_state(reset_peak=True)
                            await self.send_admin(
                                f"✅ تم تصفير التعزيز وإعادة المبلغ الأساسي إلى <b>${self.base_amount:g}</b>.",
                                self.build_main_keyboard()
                            )
                            continue

                        if self.awaiting_input == "BOOST_WINS":
                            try:
                                w_cnt = int(text)
                                if 1 <= w_cnt <= 50:
                                    self.boost_target_wins = w_cnt
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط هدف التعزيز إلى <b>{w_cnt} صفقات ناجحة</b> قبل العودة للمبلغ الأساسي.",
                                        self.build_boost_keyboard()
                                    )
                                else:
                                    await self.send_admin("⚠️ أدخل عددًا بين 1 و 50 (مثال: <code>5</code>):")
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقمًا صحيحًا (مثال: <code>5</code>):")

                        elif self.awaiting_input == "IND_BB_ZONE":
                            try:
                                pct_val = float(text.replace("%", ""))
                                if 40 <= pct_val <= 110:
                                    self.ind_bb_zone = pct_val / 100.0
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط حساسية البولينجر باند إلى <b>{int(pct_val)}%</b>.",
                                        self.build_indicators_keyboard()
                                    )
                                else:
                                    await self.send_admin("⚠️ أدخل نسبة بين 40 و 110 (مثال: <code>70</code>):")
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقم النسبة فقط (مثال: <code>70</code>):")

                        elif self.awaiting_input == "IND_RSI_LEVELS":
                            try:
                                clean_parts = text.replace("-", " ").replace("/", " ").split()
                                if len(clean_parts) == 2:
                                    os_val = float(clean_parts[0])
                                    ob_val = float(clean_parts[1])
                                    if 10 <= os_val < ob_val <= 90:
                                        self.ind_rsi_oversold = os_val
                                        self.ind_rsi_overbought = ob_val
                                        self.awaiting_input = None
                                        await self.send_admin(
                                            f"✅ تم ضبط مستويات RSI إلى: شراء ≤ <b>{int(os_val)}</b> | بيع ≥ <b>{int(ob_val)}</b>.",
                                            self.build_indicators_keyboard()
                                        )
                                    else:
                                        await self.send_admin("⚠️ تأكد أن الرقم الأول أصغر من الثاني (مثال: <code>38 62</code>):")
                                else:
                                    await self.send_admin("⚠️ أرسل رقمين مفصولين بمسافة (مثال: <code>38 62</code>):")
                            except ValueError:
                                await self.send_admin("⚠️ صيغة غير صحيحة. أرسل رقمين مثل: <code>38 62</code>:")

                        elif self.awaiting_input == "DUAL_PAIRS":
                            try:
                                cnt = int(text)
                                if 1 <= cnt <= 10:
                                    self.max_locked_pairs = cnt
                                    note = ""
                                    if self.max_concurrent_trades > cnt:
                                        self.max_concurrent_trades = cnt
                                        note = f"\n🎯 تم ضبط عدد الصفقات المتزامنة تلقائيًا إلى <b>{cnt}</b> ليتوافق مع عدد العملات."
                                    self.awaiting_input = None
                                    desc = "عملة واحدة (الوضع الفردي)" if cnt == 1 else f"مراقبة حتى <b>{cnt} عملات</b> معًا"
                                    await self.send_admin(
                                        f"✅ تم ضبط المزدوج إلى: {desc}.{note}",
                                        self.build_main_keyboard()
                                    )
                                else:
                                    await self.send_admin("⚠️ أدخل عددًا بين 1 و 10:")
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقمًا صحيحًا (مثال: <code>3</code>):")

                        elif self.awaiting_input == "CONCURRENT_TRADES":
                            try:
                                t_cnt = int(text)
                                if t_cnt < 1:
                                    await self.send_admin("⚠️ الحد الأدنى هو صفقة واحدة (1). أعد الإرسال:")
                                elif t_cnt > self.max_locked_pairs:
                                    await self.send_admin(
                                        f"⚠️ <b>خطأ:</b> عدد الصفقات (<code>{t_cnt}</code>) يجب أن يكون <b>أقل أو يساوي</b> "
                                        f"عدد العملات الممسوكة في المزدوج (<code>{self.max_locked_pairs}</code>).\n"
                                        f"أرسل رقمًا من <code>1</code> إلى <code>{self.max_locked_pairs}</code>:"
                                    )
                                else:
                                    self.max_concurrent_trades = t_cnt
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط عدد الصفقات المتزامنة إلى <b>{t_cnt} صفقات</b> (من أصل {self.max_locked_pairs} عملات ممسوكة).",
                                        self.build_main_keyboard()
                                    )
                            except ValueError:
                                await self.send_admin(f"⚠️ أرسل رقمًا صحيحًا بين 1 و {self.max_locked_pairs}:")

                        elif self.awaiting_input == "ADD_ACC_NAME":
                            self.temp_new_acc_name = text[:30]
                            self.awaiting_input = "ADD_ACC_AUTH"
                            await self.send_admin(
                                f"🔑 <b>إضافة حساب ({self.temp_new_acc_name}) — الخطوة 2 من 2:</b>\n"
                                f'أرسل الآن إطار الجلسة الكامل (<code>42["auth",...]</code>) وسيتم استخراج الآيدي وحفظ الحساب تلقائيًا:'
                            )

                        elif self.awaiting_input == "ADD_ACC_AUTH":
                            parsed = parse_auth_frame(text)
                            if parsed is not None:
                                uid = str(parsed.get("uid", "Unknown"))
                                sess = str(parsed.get("session", ""))
                                new_acc = self.register_captured_account(self.temp_new_acc_name, uid, sess, sess)
                                self.awaiting_input = None
                                self.full_reset_to_scanning("إضافة وتفعيل حساب جديد")
                                await self.send_admin(
                                    f"✅ <b>تم حفظ وتفعيل الحساب بنجاح (Demo + Live)!</b>\n"
                                    f"👤 <b>الاسم:</b> {new_acc['name']}\n"
                                    f"🆔 <b>الآيدي (UID):</b> <code>{uid}</code>",
                                    self.build_main_keyboard()
                                )
                                if self.ws:
                                    await self.ws.close()
                            else:
                                await self.send_admin('⚠️ صيغة الجلسة غير صحيحة. تأكد أنها تبدأ بـ <code>42["auth",</code> وأعد إرسالها:')

                        elif self.awaiting_input == "BASE_AMOUNT":
                            try:
                                val = float(text)
                                if val >= 1:
                                    self.base_amount = val
                                    if self.in_boost_recovery:
                                        self.effective_base_amount = val + self.boost_increment
                                    else:
                                        self.effective_base_amount = val
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط المبلغ الأساسي إلى <b>${val:g}</b>.\n"
                                        f"🌐 السلم: <code>{' ⬅️ '.join(f'${x}' for x in self.get_trade_ladder())}</code>",
                                        self.build_main_keyboard()
                                    )
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقمًا صحيحًا (مثال: <code>10</code>):")

                        elif self.awaiting_input == "BOOST_INC":
                            try:
                                inc = float(text)
                                if inc >= 1:
                                    self.boost_increment = inc
                                    if self.in_boost_recovery:
                                        self.effective_base_amount = self.base_amount + inc
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط زيادة التعزيز الواحدة إلى <b>+${inc:g}</b> للصفقات الجديدة عند الخسارة.",
                                        self.build_boost_keyboard()
                                    )
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقمًا صحيحًا (مثال: <code>2</code> أو <code>10</code>):")

                        elif self.awaiting_input == "GLOBAL_STEPS":
                            try:
                                steps = int(text)
                                if 1 <= steps <= 12:
                                    self.global_steps = steps
                                    if self.global_step_index >= steps:
                                        self.global_step_index = 0
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط التدبيل العام إلى <b>{steps} خطوات</b>.\n"
                                        f"🌐 السلم: <code>{' ⬅️ '.join(f'${x}' for x in self.get_trade_ladder())}</code>",
                                        self.build_main_keyboard()
                                    )
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقمًا صحيحًا (مثال: <code>6</code>):")

                        elif self.awaiting_input == "PAIR_STEPS":
                            try:
                                p_steps = int(text)
                                if 1 <= p_steps <= 12:
                                    self.pair_steps = p_steps
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط تدبيل الزوج الواحد إلى <b>{p_steps} محاولات</b>.",
                                        self.build_main_keyboard()
                                    )
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقمًا صحيحًا (مثال: <code>4</code>):")

                        elif self.awaiting_input == "PAYOUT":
                            try:
                                p = int(float(text.replace("%", "")))
                                if 50 <= p <= 100:
                                    self.min_payout = p
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط نسبة العائد المطلوبة إلى <b>≥ {p}%</b>.",
                                        self.build_main_keyboard()
                                    )
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقم النسبة فقط (مثال: <code>90</code>):")

                        elif self.awaiting_input == "LEAD_TIME":
                            try:
                                lt = float(text)
                                if -1.0 <= lt <= 3.0:
                                    self.entry_lead_seconds = lt
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط الفحص والدخول قبل الإغلاق إلى <b>{lt} ثانية</b>.",
                                        self.build_main_keyboard()
                                    )
                                else:
                                    await self.send_admin("⚠️ أدخل قيمة بين -1.0 و 3.0 ثانية:")
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقمًا عشريًا (مثال: <code>0.1</code>):")

                        elif self.awaiting_input == "AUTH":
                            parsed = parse_auth_frame(text)
                            if parsed is not None:
                                uid = str(parsed.get("uid", self.get_active_account()["uid"]))
                                sess = str(parsed.get("session", ""))
                                curr_name = self.get_active_account()["name"]
                                self.register_captured_account(curr_name, uid, sess, sess)
                                self.awaiting_input = None
                                await self.send_admin(
                                    f"✅ تم تحديث جلسة الحساب <b>{curr_name}</b> (<code>{uid}</code>) للديمو والحقيقي. جاري إعادة الاتصال...",
                                    self.build_main_keyboard()
                                )
                                if self.ws:
                                    await self.ws.close()
                            else:
                                await self.send_admin('⚠️ صيغة غير صحيحة. يجب أن تبدأ بـ <code>42["auth",</code>:')

            except Exception as e:
                log("TG_POLL_WARN", f"إعادة تنشيط حلقة تليجرام: {type(e).__name__}")
                await asyncio.sleep(2)

    # ==========================================
    # تصنيف الأصول، نسب العائد، والشموع
    # ==========================================
    def update_assets_payouts(self, assets_list: list):
        curr_list, crypto_list, stocks_list = [], [], []
        crypto_keys = (
            "BTC", "ETH", "LTC", "XRP", "SOL", "BNB", "DOGE", "ADA",
            "DOT", "AVAX", "LINK", "MATIC", "TON", "TRON", "TRX", "DASH", "BITB"
        )

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

            upper_sym = symbol.upper()
            upper_name = disp_name.upper()

            if "crypto" in asset_type or any(upper_sym.startswith(k) or k in upper_name for k in crypto_keys):
                crypto_list.append(symbol)
            elif "stock" in asset_type or symbol.startswith("#") or "MARA" in upper_sym:
                stocks_list.append(symbol)
            elif "currency" in asset_type or (symbol.endswith("_otc") and len(symbol) == 10):
                curr_list.append(symbol)

        if curr_list:
            self.assets_by_cat["CURRENCIES"] = curr_list
        if crypto_list:
            self.assets_by_cat["CRYPTO"] = crypto_list
        if stocks_list:
            self.assets_by_cat["STOCKS"] = stocks_list

        log(
            "ASSETS",
            f"الأصول المفتوحة => العملات: {len(self.assets_by_cat['CURRENCIES'])} | "
            f"الكريبتو: {len(self.assets_by_cat['CRYPTO'])} | الأسهم: {len(self.assets_by_cat['STOCKS'])}"
        )

    def is_pair_eligible(self, asset: str) -> tuple[bool, int]:
        payout = self.payouts.get(asset)
        if payout is None:
            return False, 0
        if self.open_assets and asset not in self.open_assets:
            return False, payout
        return (payout >= self.min_payout), payout

    def update_candle_from_history(self, asset: str, rows: list):
        if not rows:
            return
        if asset not in self.candles:
            self.candles[asset] = {}

        valid_rows = [r for r in rows if isinstance(r, (list, dict))]
        if valid_rows and isinstance(valid_rows[0], list):
            valid_rows.sort(key=lambda x: normalize_ts(x[0]))

        tick_buckets = {}
        for row in valid_rows:
            if isinstance(row, list):
                if len(row) >= 3:
                    ts = normalize_ts(row[0])
                    minute_ts = (int(ts) // 60) * 60
                    o_p, c_p = float(row[1]), float(row[2])
                    h_p = float(row[3]) if len(row) >= 5 else max(o_p, c_p)
                    l_p = float(row[4]) if len(row) >= 5 else min(o_p, c_p)
                    self.candles[asset][minute_ts] = {"open": o_p, "close": c_p, "high": h_p, "low": l_p}
                elif len(row) == 2:
                    ts = normalize_ts(row[0])
                    price = float(row[1])
                    minute_ts = (int(ts) // 60) * 60
                    if minute_ts not in tick_buckets:
                        tick_buckets[minute_ts] = {"open": price, "close": price, "high": price, "low": price}
                    else:
                        tick_buckets[minute_ts]["close"] = price
                        tick_buckets[minute_ts]["high"] = max(tick_buckets[minute_ts]["high"], price)
                        tick_buckets[minute_ts]["low"] = min(tick_buckets[minute_ts]["low"], price)
            elif isinstance(row, dict) and "time" in row:
                ts = normalize_ts(row["time"])
                minute_ts = (int(ts) // 60) * 60
                o_p, c_p = float(row["open"]), float(row["close"])
                h_p = float(row.get("high", max(o_p, c_p)))
                l_p = float(row.get("low", min(o_p, c_p)))
                self.candles[asset][minute_ts] = {"open": o_p, "close": c_p, "high": h_p, "low": l_p}

        for m_ts, ohlc in tick_buckets.items():
            if m_ts in self.candles[asset]:
                self.candles[asset][m_ts]["open"] = ohlc["open"]
                self.candles[asset][m_ts]["high"] = max(self.candles[asset][m_ts].get("high", ohlc["high"]), ohlc["high"])
                self.candles[asset][m_ts]["low"] = min(self.candles[asset][m_ts].get("low", ohlc["low"]), ohlc["low"])
            else:
                self.candles[asset][m_ts] = ohlc

    def update_candle_from_tick(self, asset: str, raw_ts, price: float):
        if asset not in self.candles:
            self.candles[asset] = {}
        ts = normalize_ts(raw_ts)

        raw_diff = ts - time.time()
        self.server_hour_shift = round(raw_diff / 3600.0) * 3600.0

        self.latest_prices[asset] = price

        minute_ts = (int(ts) // 60) * 60
        bucket = self.candles[asset].get(minute_ts)
        if bucket is None:
            self.candles[asset][minute_ts] = {"open": price, "close": price, "high": price, "low": price}
        else:
            bucket["close"] = price
            bucket["high"] = max(bucket.get("high", price), price)
            bucket["low"] = min(bucket.get("low", price), price)

        if len(self.candles[asset]) > 140:
            oldest_keys = sorted(self.candles[asset].keys())[:-120]
            for k in oldest_keys:
                del self.candles[asset][k]

    def get_last_five(self, asset: str):
        pair_candles = self.candles.get(asset, {})
        sorted_minutes = sorted(pair_candles.keys())
        if len(sorted_minutes) < 5:
            return None

        m4, m3, m2, m1, m_open = sorted_minutes[-5:]
        c4 = candle_color(pair_candles[m4]["open"], pair_candles[m4]["close"])
        c3 = candle_color(pair_candles[m3]["open"], pair_candles[m3]["close"])
        c2 = candle_color(pair_candles[m2]["open"], pair_candles[m2]["close"])
        c1 = candle_color(pair_candles[m1]["open"], pair_candles[m1]["close"])
        c_open = candle_color(pair_candles[m_open]["open"], pair_candles[m_open]["close"])
        return (m4, m3, m2, m1, m_open), (c4, c3, c2, c1, c_open)

    def print_five_candles(self, asset: str, force: bool = False):
        data = self.get_last_five(asset)
        if data is None:
            return

        (m4, m3, m2, m1, m_open), (c4, c3, c2, c1, c_open) = data
        payout = self.payouts.get(asset, 0)
        state_signature = (m_open, c4, c3, c2, c1, c_open, payout)
        if not force and self.last_logged_state.get(asset) == state_signature:
            return
        self.last_logged_state[asset] = state_signature

        shift = self.server_hour_shift
        t4 = datetime.fromtimestamp(m4 - shift, tz=TZ_UTC3).strftime("%H:%M")
        t3 = datetime.fromtimestamp(m3 - shift, tz=TZ_UTC3).strftime("%H:%M")
        t2 = datetime.fromtimestamp(m2 - shift, tz=TZ_UTC3).strftime("%H:%M")
        t1 = datetime.fromtimestamp(m1 - shift, tz=TZ_UTC3).strftime("%H:%M")
        t_open = datetime.fromtimestamp(m_open - shift, tz=TZ_UTC3).strftime("%H:%M")

        log(
            "5-CANDLES",
            f"{asset} ({payout}%) => "
            f"[1({t4}): {color_label(c4)}] | "
            f"[2({t3}): {color_label(c3)}] | "
            f"[3({t2}): {color_label(c2)}] | "
            f"[4({t1}): {color_label(c1)}] || "
            f"[5-المفتوحة({t_open}): {color_label(c_open)}]"
        )

    # ==========================================
    # التنفيذ الفوري، الوضع السريع، الصفقات المتزامنة، والتدبيل
    # ==========================================
    async def open_trade(
        self,
        asset: str,
        direction: str,
        g_idx: int,
        p_idx: int,
        trade_minute: int,
        signal_uid: int | None,
        is_draw_retry: bool = False,
        is_cont_entry: bool = False,
        cycle_base: float | None = None
    ):
        if not self.ws or not self.authenticated or not self.is_running:
            return

        if cycle_base is None:
            cycle_base = self.get_current_entry_base()

        ladder = self.get_trade_ladder(custom_base=cycle_base)
        if not self.martingale_enabled:
            g_idx = 0
        else:
            g_idx = g_idx % len(ladder)

        amount = ladder[g_idx]
        req_id = int(time.time() * 1000) + self.trade_seq
        is_demo_flag = 1 if self.is_demo_mode else 0

        await self.ensure_subscribed(asset)

        self.trade_seq += 1
        entry_price = self.latest_prices.get(asset)
        close_ts = self.get_server_now() + TRADE_DURATION + MARTINGALE_OFFSET

        sig_sym = self.format_signal_symbol(asset)
        dir_text = "🟢 CALL UP 📈" if direction == "call" else "🔴 PUT DOWN 📉"

        if is_cont_entry:
            cont_msg = (
                f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                f"┃ 🔄 <b>مواصلة النموذج (دخول عكسي)</b>\n"
                f"┃ 🏛 <code>{sig_sym}</code>\n"
                f"┃ ⏱ Expiry  M1\n"
                f"┃ {dir_text}\n"
                f"╰━━━━━━━━━━━━━━━╯"
            )
            signal_uid = self.enqueue_new_signal(cont_msg)
        elif p_idx == 0 and not is_draw_retry:
            confirm_msg = (
                f"🚀 <b>تم تأكيد الدخول في الصفقة ✅</b>\n"
                f"🏛 <code>{sig_sym}</code> | {dir_text}"
            )
            self.enqueue_signal_reply(signal_uid, confirm_msg)

        trade_obj = {
            "trade_id": self.trade_seq,
            "asset": asset,
            "direction": direction,
            "g_idx": g_idx,
            "p_idx": p_idx,
            "trade_minute": trade_minute,
            "close_ts": close_ts,
            "entry_price": entry_price,
            "price_confirmed": False,
            "amount": amount,
            "cycle_base": cycle_base,
            "signal_uid": signal_uid,
            "is_cont": is_cont_entry,
        }
        self.active_trades.append(trade_obj)
        self.refresh_mode_state()

        self.total_turnover += amount
        self.current_cycle_spent += amount

        order_payload = {
            "asset": asset,
            "amount": amount,
            "action": direction,
            "isDemo": is_demo_flag,
            "requestId": req_id,
            "optionType": 100,
            "time": TRADE_DURATION
        }

        await self.ws.send(f'42["openOrder",{json.dumps(order_payload)}]')

        mode_tag = "DEMO" if self.is_demo_mode else "LIVE"
        tag_note = " (إعادة بعد تعادل ⚖️)" if is_draw_retry else (" (مواصلة النموذج 🔄)" if is_cont_entry else "")
        log(
            "TRADE_EXEC",
            f"⚡ تنفيذ [{mode_tag}]{tag_note} | {sig_sym} | {dir_text} | المبلغ: ${amount} (أساس الدورة: ${cycle_base:g}) | "
            f"سعر الفتح: {entry_price} | [عام {g_idx + 1}/{len(ladder)} | زوج {p_idx + 1}/{self.pair_steps}]"
        )

    def evaluate_trade_result(self, trade: dict) -> str:
        asset = trade["asset"]
        direction = trade["direction"]
        current_price = self.latest_prices.get(asset)
        entry_price = trade["entry_price"]

        if current_price is not None and entry_price is not None:
            if current_price == entry_price:
                return "DRAW"
            if direction == "put":
                return "WIN" if current_price < entry_price else "LOSS"
            else:
                return "WIN" if current_price > entry_price else "LOSS"

        candle = self.candles.get(asset, {}).get(trade["trade_minute"])
        if candle is not None:
            if candle["close"] == candle["open"]:
                return "DRAW"
            if direction == "put":
                return "WIN" if candle["close"] < candle["open"] else "LOSS"
            else:
                return "WIN" if candle["close"] > candle["open"] else "LOSS"

        return "LOSS"

    async def precision_entry_watcher(self):
        while True:
            if not self.is_running or (not self.locked_candidates and not self.active_trades):
                await asyncio.sleep(0.05)
                continue

            server_now = self.get_server_now()

            deadlines = []
            if self.active_trades:
                deadlines.append(min(t["close_ts"] for t in self.active_trades))
            if self.locked_candidates:
                earliest_minute = min(c["target_minute"] for c in self.locked_candidates)
                deadlines.append(earliest_minute + 60.0 + SERVER_CLOCK_OFFSET)

            min_remaining = min(deadlines) - server_now
            if min_remaining > 2.0:
                await asyncio.sleep(0.05)
                continue
            elif min_remaining > self.entry_lead_seconds:
                await asyncio.sleep(0.001)
                continue

            # ========================================================
            # 1) فحص الصفقات النشطة التي حان إغلاقها الآن
            # ========================================================
            if self.active_trades:
                due_trades = [t for t in self.active_trades if (t["close_ts"] - server_now) <= self.entry_lead_seconds]

                for trade in due_trades:
                    if trade in self.active_trades:
                        self.active_trades.remove(trade)

                    asset = trade["asset"]
                    sig_sym = self.format_signal_symbol(asset)
                    sig_uid = trade["signal_uid"]
                    direction = trade["direction"]
                    g_idx = trade["g_idx"]
                    p_idx = trade["p_idx"]
                    won_amount = trade["amount"]
                    trade_cycle_base = trade["cycle_base"]
                    is_cont = trade["is_cont"]

                    result = self.evaluate_trade_result(trade)
                    attempt_num = g_idx + 1
                    sup_num = to_superscript(attempt_num)
                    curr_p = self.latest_prices.get(asset)
                    next_minute = (trade["trade_minute"] or int(server_now // 60) * 60) + 60
                    ladder = self.get_trade_ladder(custom_base=trade_cycle_base)

                    log(
                        "TRADE_CHECK",
                        f"⏱ فحص الصفقة قبل الإغلاق | {asset} | "
                        f"دخول: {trade['entry_price']} vs حالي: {curr_p} => {result} "
                        f"[عام {attempt_num}/{len(ladder)} | زوج {p_idx + 1}/{self.pair_steps}]"
                    )

                    # أ) حالة الربح (WIN)
                    if result == "WIN":
                        self.record_trade_outcome(sig_sym, is_win=True, attempt_num=attempt_num)
                        win_msg = (
                            f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                            f"┃ 🏛 <code>{sig_sym}</code>\n"
                            f"┃ 🏆 <b>WIN{sup_num} ✅</b>\n"
                            f"╰━━━━━━━━━━━━━━━╯"
                        )
                        self.enqueue_signal_reply(sig_uid, win_msg)

                        if not any(t["g_idx"] > 0 for t in self.active_trades):
                            self.global_step_index = 0
                        self.current_cycle_spent = 0.0

                        if self.boost_enabled and self.in_boost_recovery:
                            if self.boost_mode == "TRADES":
                                self.boost_wins_done += 1
                                if self.boost_wins_done >= self.boost_target_wins:
                                    log("BOOST_DONE", f"🎉 اكتملت صفقات التعزيز الناجحة ({self.boost_wins_done}/{self.boost_target_wins})! العودة للأساسي ${self.base_amount:g}")
                                    asyncio.create_task(self.send_admin(
                                        f"🎉 <b>اكتمل عدد صفقات التعزيز الناجحة ({self.boost_wins_done}/{self.boost_target_wins})!</b>\n"
                                        f"💵 عاد المبلغ الأساسي تلقائيًا إلى <b>${self.base_amount:g}</b>."
                                    ))
                                    self.reset_boost_state(reset_peak=False)
                                else:
                                    log("BOOST_PROG", f"🛡️ تقدم التعزيز: تم إنجاز {self.boost_wins_done}/{self.boost_target_wins} صفقات ناجحة")

                            elif self.boost_mode == "CAPITAL":
                                payout_pct = self.payouts.get(asset, self.min_payout)
                                if self.account_balance is not None and self.peak_balance is not None:
                                    projected_bal = float(self.account_balance) + won_amount * (1.0 + payout_pct / 100.0)
                                    if projected_bal >= (self.peak_balance - 0.01):
                                        self.peak_balance = max(self.peak_balance, projected_bal)
                                        log("BOOST_DONE", f"🎉 وصل رأس المال للقمة (${format_money(self.peak_balance)})! العودة للأساسي ${self.base_amount:g}")
                                        asyncio.create_task(self.send_admin(
                                            f"🎉 <b>تم تعويض رأس المال والعودة إلى القمة (${format_money(self.peak_balance)})!</b>\n"
                                            f"💵 عاد المبلغ الأساسي تلقائيًا إلى <b>${self.base_amount:g}</b>."
                                        ))
                                        self.reset_boost_state(reset_peak=False)
                                    else:
                                        log("BOOST_PROG", f"🛡️ تقدم التعزيز نحو القمة: المتوقع ${format_money(projected_bal)} / الهدف ${format_money(self.peak_balance)}")

                        if self.continuation_enabled and attempt_num == 1 and not is_cont:
                            eligible, _ = self.is_pair_eligible(asset)
                            if eligible:
                                opp_dir = "call" if direction == "put" else "put"
                                log("CONT_EXEC", f"🔄 تفعيل مواصلة النموذج على {sig_sym} — دخول عكسي ({opp_dir.upper()})")
                                await self.open_trade(
                                    asset,
                                    opp_dir,
                                    g_idx=0,
                                    p_idx=0,
                                    trade_minute=next_minute,
                                    signal_uid=None,
                                    is_cont_entry=True,
                                    cycle_base=None
                                )
                        continue

                    # ب) حالة التعادل (DRAW)
                    if result == "DRAW":
                        self.current_cycle_spent -= won_amount
                        log("DRAW", f"⚖️ تعادل على {sig_sym} — إعادة الدخول بنفس المبلغ (${won_amount}) دون تدبيل.")
                        await self.open_trade(
                            asset,
                            direction,
                            g_idx=g_idx,
                            p_idx=p_idx,
                            trade_minute=next_minute,
                            signal_uid=sig_uid,
                            is_draw_retry=True,
                            is_cont_entry=is_cont,
                            cycle_base=trade_cycle_base
                        )
                        continue

                    # ج) حالة الخسارة (LOSS)
                    if not self.martingale_enabled:
                        self.record_trade_outcome(sig_sym, is_win=False, attempt_num=1)
                        loss_msg = (
                            f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                            f"┃ 🏛 <code>{sig_sym}</code>\n"
                            f"┃ ❌ <b>LOSS</b>\n"
                            f"╰━━━━━━━━━━━━━━━╯"
                        )
                        self.enqueue_signal_reply(sig_uid, loss_msg)

                        if self.boost_enabled and not self.in_boost_recovery:
                            self.in_boost_recovery = True
                            self.boost_wins_done = 0
                            if self.peak_balance is None and self.account_balance is not None:
                                self.peak_balance = float(self.account_balance) + self.current_cycle_spent
                            self.effective_base_amount = self.base_amount + self.boost_increment
                            self._notify_boost_activated()

                        self.current_cycle_spent = 0.0
                        continue

                    next_g_idx = g_idx + 1
                    next_p_idx = p_idx + 1

                    if next_g_idx >= self.global_steps:
                        self.record_trade_outcome(sig_sym, is_win=False, attempt_num=attempt_num)
                        loss_msg = (
                            f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                            f"┃ 🏛 <code>{sig_sym}</code>\n"
                            f"┃ ❌ <b>LOSS</b>\n"
                            f"╰━━━━━━━━━━━━━━━╯"
                        )
                        self.enqueue_signal_reply(sig_uid, loss_msg)

                        if self.boost_enabled and not self.in_boost_recovery:
                            self.in_boost_recovery = True
                            self.boost_wins_done = 0
                            if self.peak_balance is None and self.account_balance is not None:
                                self.peak_balance = float(self.account_balance) + self.current_cycle_spent
                            self.effective_base_amount = self.base_amount + self.boost_increment
                            self._notify_boost_activated()

                        self.current_cycle_spent = 0.0
                        self.global_step_index = 0
                        if next_p_idx < self.pair_steps:
                            await self.open_trade(
                                asset, direction,
                                g_idx=0, p_idx=next_p_idx,
                                trade_minute=next_minute,
                                signal_uid=sig_uid,
                                cycle_base=None
                            )
                    else:
                        if next_p_idx < self.pair_steps:
                            self.global_step_index = next_g_idx
                            await self.open_trade(
                                asset, direction,
                                g_idx=next_g_idx, p_idx=next_p_idx,
                                trade_minute=next_minute,
                                signal_uid=sig_uid,
                                is_cont_entry=is_cont,
                                cycle_base=trade_cycle_base
                            )
                        else:
                            self.global_step_index = next_g_idx
                            self.pending_martingale_steps.append((next_g_idx, trade_cycle_base))
                            log(
                                "PAIR_SWITCH",
                                f"اكتمال {self.pair_steps} محاولات على {sig_sym} — ترحيل التدبيل إلى الخطوة {next_g_idx + 1}/{self.global_steps} (أساس الدورة: ${trade_cycle_base:g})"
                            )

            # ========================================================
            # 2) فحص الأزواج المجهّزة في قائمة الانتظار
            # ========================================================
            if self.locked_candidates:
                earliest_minute = min(c["target_minute"] for c in self.locked_candidates)
                cand_remaining = (earliest_minute + 60.0 + SERVER_CLOCK_OFFSET) - server_now

                if cand_remaining <= self.entry_lead_seconds:
                    due_candidates = [c for c in self.locked_candidates if c["target_minute"] <= earliest_minute]
                    future_candidates = [c for c in self.locked_candidates if c["target_minute"] > earliest_minute]

                    selected_to_enter = []
                    max_to_open = min(self.max_concurrent_trades, self.max_locked_pairs)

                    for cand in due_candidates:
                        asset = cand["asset"]
                        sig_sym = self.format_signal_symbol(asset)
                        sig_uid = cand["signal_uid"]
                        direction = cand["direction"]
                        target_min = cand["target_minute"]
                        rank = cand["rank"]

                        if len(selected_to_enter) >= max_to_open:
                            log("MULTI_LIMIT", f"إلغاء [#{rank}] {sig_sym} لاكتمال الحد المسموح للصفقات المتزامنة ({max_to_open}).")
                            self.enqueue_signal_reply(
                                sig_uid,
                                f"⚪ <b>تم إلغاء الإشارة (اكتمل عدد الصفقات المتزامنة {max_to_open})</b>\n"
                                f"🏛 <code>{sig_sym}</code>"
                            )
                            continue

                        eligible, payout = self.is_pair_eligible(asset)
                        if not eligible:
                            log("SKIP_PAYOUT", f"[#{rank}] انخفض عائد {asset} إلى {payout}% — إلغاء الإشارة")
                            self.enqueue_signal_reply(
                                sig_uid,
                                f"⚪ <b>تم إلغاء الإشارة</b>\n"
                                f"🏛 <code>{sig_sym}</code>"
                            )
                            continue

                        target_candle = self.candles.get(asset, {}).get(target_min)
                        if not target_candle:
                            log("SKIP_DATA", f"[#{rank}] فقدان بيانات شمعة {asset} — إلغاء الإشارة")
                            self.enqueue_signal_reply(
                                sig_uid,
                                f"⚪ <b>تم إلغاء الإشارة</b>\n"
                                f"🏛 <code>{sig_sym}</code>"
                            )
                            continue

                        live_color = candle_color(target_candle["open"], target_candle["close"])
                        condition_met = (
                            (direction == "put" and live_color == "GREEN") or
                            (direction == "call" and live_color == "RED")
                        )

                        ind_ok, ind_reason = (True, "متوقف")
                        if condition_met and self.indicators_enabled:
                            ind_ok, ind_reason = self.check_indicators(asset, direction, is_pre_close=True)

                        log(
                            "PRE_CLOSE",
                            f"⏱ فحص [#{rank}] قبل الإغلاق بـ {max(0.0, cand_remaining):.3f}ث | {asset} | "
                            f"{color_label(live_color)} | المؤشرات: {ind_reason} => "
                            f"{'دخول ✅' if (condition_met and ind_ok) else 'إلغاء ❌'}"
                        )

                        if condition_met and ind_ok:
                            selected_to_enter.append(cand)
                        else:
                            cancel_note = f" ({ind_reason})" if (condition_met and not ind_ok) else ""
                            self.enqueue_signal_reply(
                                sig_uid,
                                f"⚪ <b>تم إلغاء الإشارة{cancel_note}</b>\n"
                                f"🏛 <code>{sig_sym}</code>"
                            )

                    if selected_to_enter:
                        for other in future_candidates:
                            other_sym = self.format_signal_symbol(other["asset"])
                            self.enqueue_signal_reply(
                                other["signal_uid"],
                                f"⚪ <b>تم إلغاء الإشارة</b>\n"
                                f"🏛 <code>{other_sym}</code>"
                            )

                        self.locked_candidates.clear()

                        for cand in selected_to_enter:
                            if self.martingale_enabled:
                                if self.pending_martingale_steps:
                                    step_to_use, carried_base = self.pending_martingale_steps.pop(0)
                                elif not any(t["g_idx"] > 0 for t in self.active_trades):
                                    step_to_use = self.global_step_index
                                    carried_base = None
                                else:
                                    step_to_use = 0
                                    carried_base = None
                            else:
                                step_to_use = 0
                                carried_base = None

                            next_trade_minute = cand["target_minute"] + 60
                            await self.open_trade(
                                cand["asset"],
                                cand["direction"],
                                g_idx=step_to_use,
                                p_idx=0,
                                trade_minute=next_trade_minute,
                                signal_uid=cand["signal_uid"],
                                cycle_base=carried_base
                            )
                    else:
                        if future_candidates:
                            self.locked_candidates = future_candidates
                        else:
                            self.locked_candidates.clear()
                            if not self.active_trades:
                                self.switch_pair_keep_global("لم تستوفِ أي من الأزواج الممسوكة الشرط")

            self.refresh_mode_state()

    def _notify_boost_activated(self):
        if self.boost_mode == "CAPITAL":
            peak_txt = f"${format_money(self.peak_balance)}" if self.peak_balance is not None else "غير متاح"
            bal_txt = f"${format_money(self.account_balance)}" if self.account_balance is not None else "—"
            log(
                "BOOST_UP",
                f"🛡️ تفعيل التعزيز للصفقات الجديدة (تعويض القمة)! الأساسي المعزّز: ${self.effective_base_amount:g} | الرصيد: {bal_txt} / القمة: {peak_txt}"
            )
            asyncio.create_task(self.send_admin(
                f"🛡️ <b>تم تفعيل التعزيز للصفقات الجديدة (تعويض القمة)!</b>\n"
                f"💵 المبلغ الأساسي الجديد: <b>${self.effective_base_amount:g}</b> (يُطبَّق على الصفقات الجديدة فقط)\n"
                f"💰 الرصيد الحالي: <code>{bal_txt}</code> ⬅️ القمة المستهدفة: <code>{peak_txt}</code>"
            ))
        else:
            log(
                "BOOST_UP",
                f"🛡️ تفعيل التعزيز للصفقات الجديدة (عدد صفقات)! الأساسي المعزّز: ${self.effective_base_amount:g} | الهدف: {self.boost_target_wins} صفقات ناجحة"
            )
            asyncio.create_task(self.send_admin(
                f"🛡️ <b>تم تفعيل التعزيز للصفقات الجديدة (عدد صفقات)!</b>\n"
                f"💵 المبلغ الأساسي الجديد: <b>${self.effective_base_amount:g}</b> (يُطبَّق على الصفقات الجديدة فقط)\n"
                f"🏆 المطلوب: <code>{self.boost_target_wins} صفقات ناجحة</code> للعودة للأساسي الأصلي."
            ))

    async def process_market_logic(self, asset: str, force_print: bool = False):
        if not self.is_running:
            return

        locked_assets = {c["asset"] for c in self.locked_candidates}
        active_assets = {t["asset"] for t in self.active_trades}
        if asset not in self.get_active_scan_pairs() and asset not in locked_assets and asset not in active_assets:
            return

        eligible, payout = self.is_pair_eligible(asset)
        if not eligible:
            return

        self.print_five_candles(asset, force=force_print)

        can_capture_more = (
            len(self.locked_candidates) < self.max_locked_pairs
            and (self.fast_mode or not self.active_trades)
        )

        if can_capture_more:
            if asset in locked_assets or asset in active_assets:
                return

            data = self.get_last_five(asset)
            if data is None:
                return
            (m4, m3, m2, m1, m_open), (c4, c3, c2, c1, c_open) = data

            server_now = self.get_server_now()
            if (m_open + 60.0 + SERVER_CLOCK_OFFSET - server_now) <= (self.entry_lead_seconds + 0.5):
                return

            sig_sym = self.format_signal_symbol(asset)
            real_entry_ts = (m_open + 60) - self.server_hour_shift
            entry_time_str = datetime.fromtimestamp(real_entry_ts, tz=TZ_UTC3).strftime("%H:%M:%S")
            rank = len(self.locked_candidates) + 1
            priority_line = f"┃ 🔢 الأولوية  <b>#{rank}</b>\n" if self.max_locked_pairs > 1 else ""

            # شرط البيع: أحمر -> أحمر -> أخضر -> أحمر
            if c4 == "RED" and c3 == "RED" and c2 == "GREEN" and c1 == "RED":
                if self.indicators_enabled:
                    ind_ok, ind_reason = self.check_indicators(asset, "put", is_pre_close=False)
                    if not ind_ok:
                        skip_sig = (m_open, "put", ind_reason)
                        if self.last_ind_skip_log.get(asset) != skip_sig:
                            self.last_ind_skip_log[asset] = skip_sig
                            log("IND_FILTER", f"📊 تخطي نموذج بيع على {sig_sym} بسبب المؤشرات: {ind_reason}")
                        return

                signal_msg = (
                    f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                    f"{priority_line}"
                    f"┃ 🏛 <code>{sig_sym}</code>\n"
                    f"┃ ⏰ Entry  <code>{entry_time_str}</code> [UTC+3]\n"
                    f"┃ ⏱ Expiry  M1\n"
                    f"┃ 🔴 <b>PUT DOWN 📉</b>\n"
                    f"┃ 📌 إذا أغلقت خضراء 🟢 ⬅️ بيع\n"
                    f"┃ ⚠️ إذا أغلقت حمراء 🔴 ⬅️ لا تدخل\n"
                    f"╰━━━━━━━━━━━━━━━╯"
                )
                sig_uid = self.enqueue_new_signal(signal_msg)
                self.locked_candidates.append({
                    "asset": asset,
                    "direction": "put",
                    "target_minute": m_open,
                    "signal_uid": sig_uid,
                    "rank": rank
                })
                self.refresh_mode_state()
                asyncio.create_task(self.ensure_subscribed(asset))
                log(
                    "LOCK",
                    f"🎯 رصد نموذج بيع [#{rank}/{self.max_locked_pairs}] على {sig_sym} ({payout}%) | الدخول: {entry_time_str} [UTC+3]"
                )

            # شرط الشراء: أخضر -> أخضر -> أحمر -> أخضر
            elif c4 == "GREEN" and c3 == "GREEN" and c2 == "RED" and c1 == "GREEN":
                if self.indicators_enabled:
                    ind_ok, ind_reason = self.check_indicators(asset, "call", is_pre_close=False)
                    if not ind_ok:
                        skip_sig = (m_open, "call", ind_reason)
                        if self.last_ind_skip_log.get(asset) != skip_sig:
                            self.last_ind_skip_log[asset] = skip_sig
                            log("IND_FILTER", f"📊 تخطي نموذج شراء على {sig_sym} بسبب المؤشرات: {ind_reason}")
                        return

                signal_msg = (
                    f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                    f"{priority_line}"
                    f"┃ 🏛 <code>{sig_sym}</code>\n"
                    f"┃ ⏰ Entry  <code>{entry_time_str}</code> [UTC+3]\n"
                    f"┃ ⏱ Expiry  M1\n"
                    f"┃ 🟢 <b>CALL UP 📈</b>\n"
                    f"┃ 📌 إذا أغلقت حمراء 🔴 ⬅️ شراء\n"
                    f"┃ ⚠️ إذا أغلقت خضراء 🟢 ⬅️ لا تدخل\n"
                    f"╰━━━━━━━━━━━━━━━╯"
                )
                sig_uid = self.enqueue_new_signal(signal_msg)
                self.locked_candidates.append({
                    "asset": asset,
                    "direction": "call",
                    "target_minute": m_open,
                    "signal_uid": sig_uid,
                    "rank": rank
                })
                self.refresh_mode_state()
                asyncio.create_task(self.ensure_subscribed(asset))
                log(
                    "LOCK",
                    f"🎯 رصد نموذج شراء [#{rank}/{self.max_locked_pairs}] على {sig_sym} ({payout}%) | الدخول: {entry_time_str} [UTC+3]"
                )

    async def handle_payload(self, event_name: str, data):
        if event_name == "successauth":
            self.authenticated = True
            acc = self.get_active_account()
            mode_label = "التجريبي (Demo)" if self.is_demo_mode else "الحقيقي (Live)"
            log("AUTH", f"تم تأكيد المصادقة للحساب [{acc['name']} | {acc['uid']}] في الوضع {mode_label} ✅")
            return

        if event_name == "updateAssets" and isinstance(data, list):
            self.update_assets_payouts(data)
            return

        if event_name == "successupdateBalance" and isinstance(data, dict):
            msg_is_demo = data.get("isDemo")
            if msg_is_demo is None or bool(msg_is_demo) == self.is_demo_mode:
                new_bal = float(data.get("balance", 0))
                self.account_balance = new_bal
                mode_label = "الديمو" if self.is_demo_mode else "الحقيقي"
                log("BALANCE", f"رصيد الحساب ({mode_label}) الحالي: ${format_money(new_bal)}")

                if self.peak_balance is None:
                    self.peak_balance = new_bal
                elif new_bal >= self.peak_balance:
                    self.peak_balance = new_bal
                    if self.boost_enabled and self.in_boost_recovery and self.boost_mode == "CAPITAL":
                        log("BOOST_DONE", f"🎉 وصل رصيد الحساب للقمة (${format_money(self.peak_balance)})! العودة للأساسي ${self.base_amount:g}")
                        asyncio.create_task(self.send_admin(
                            f"🎉 <b>تم تعويض رأس المال والعودة إلى القمة (${format_money(self.peak_balance)})!</b>\n"
                            f"💵 عاد المبلغ الأساسي تلقائيًا إلى <b>${self.base_amount:g}</b>."
                        ))
                        self.reset_boost_state(reset_peak=False)
            return

        if event_name == "successopenOrder" and isinstance(data, dict):
            order_asset = data.get("asset")
            open_p = data.get("openPrice")
            if open_p is not None:
                matched = False
                for t in reversed(self.active_trades):
                    if t["asset"] == order_asset and not t["price_confirmed"]:
                        t["entry_price"] = float(open_p)
                        t["price_confirmed"] = True
                        matched = True
                        break
                if not matched and self.active_trades:
                    self.active_trades[-1]["entry_price"] = float(open_p)

            log(
                "ORDER_OK",
                f"تأكيد السيرفر | {order_asset} | ID: {data.get('id')} | المبلغ: ${data.get('amount')} | "
                f"سعر الفتح الفعلي: {open_p}"
            )
            return

        if event_name == "updateStream":
            if isinstance(data, list):
                locked_assets = {c["asset"] for c in self.locked_candidates}
                active_assets = {t["asset"] for t in self.active_trades}
                for item in data:
                    if isinstance(item, list) and len(item) >= 3:
                        asset, ts, price = str(item[0]), item[1], float(item[2])
                        self.update_candle_from_tick(asset, ts, price)
                        if self.fast_mode or self.mode == "SCANNING" or asset in locked_assets or asset in active_assets:
                            await self.process_market_logic(asset, force_print=False)
            return

        if isinstance(data, dict):
            asset = data.get("asset", self.active_symbol)
            candles_rows = data.get("candles") or []
            history_rows = data.get("history") or []
            if history_rows:
                self.update_candle_from_history(asset, history_rows)
            if candles_rows:
                self.update_candle_from_history(asset, candles_rows)
            if history_rows or candles_rows:
                await self.process_market_logic(asset, force_print=True)
        elif isinstance(data, list) and len(data) > 0 and event_name in (
            "history", "loadHistoryPeriod", "loadHistoryPeriodFast", "updateHistoryNew", "updateHistoryNewFast"
        ):
            self.update_candle_from_history(self.active_symbol, data)
            await self.process_market_logic(self.active_symbol, force_print=True)

    async def keepalive_heartbeat_loop(self, ws):
        while True:
            await asyncio.sleep(12)
            if not self.authenticated:
                continue
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

        idx = 0
        focus_idx = 0
        while True:
            if not self.is_running:
                await asyncio.sleep(1)
                continue

            if not self.fast_mode and self.active_trades:
                trade_assets = list(dict.fromkeys(t["asset"] for t in self.active_trades))
                pair = trade_assets[focus_idx % len(trade_assets)]
                focus_idx += 1
                self.active_symbol = pair
                try:
                    await ws.send(f'42["changeSymbol",{{"asset":"{pair}","period":60}}]')
                except Exception:
                    break
                await asyncio.sleep(0.8 if len(trade_assets) > 1 else 2.0)
                continue

            if len(self.locked_candidates) >= self.max_locked_pairs:
                cand_assets = [c["asset"] for c in self.locked_candidates]
                pair = cand_assets[focus_idx % len(cand_assets)]
                focus_idx += 1
                self.active_symbol = pair
                try:
                    await ws.send(f'42["changeSymbol",{{"asset":"{pair}","period":60}}]')
                except Exception:
                    break
                await asyncio.sleep(0.8 if len(cand_assets) > 1 else 1.5)
                continue

            active_pairs = self.get_active_scan_pairs()
            if not active_pairs:
                await asyncio.sleep(1)
                continue

            locked_assets = {c["asset"] for c in self.locked_candidates}
            active_assets = {t["asset"] for t in self.active_trades}
            pair = active_pairs[idx % len(active_pairs)]
            idx += 1

            if pair in locked_assets or pair in active_assets:
                await asyncio.sleep(0.05)
                continue

            eligible, payout = self.is_pair_eligible(pair)
            if self.payouts and not eligible:
                await asyncio.sleep(0.05)
                continue

            if pair not in self.subscribed_pairs:
                try:
                    await ws.send(f'42["subfor","{pair}"]')
                    self.subscribed_pairs.add(pair)
                except Exception:
                    break

            self.active_symbol = pair
            self.req_index += 1
            now_ts = int(self.get_server_now())
            try:
                log("SCAN", f"--- فحص [{self.get_scope_label()}]: {pair} ({payout}%) ---")
                await ws.send(f'42["changeSymbol",{{"asset":"{pair}","period":60}}]')
                history_req = {
                    "asset": pair,
                    "index": self.req_index,
                    "time": now_ts,
                    "offset": 3600,
                    "period": 60
                }
                await ws.send(f'42["loadHistoryPeriod",{json.dumps(history_req)}]')
            except Exception:
                break

            await asyncio.sleep(SCAN_INTERVAL_PER_PAIR)
            if self.is_running and len(self.locked_candidates) < self.max_locked_pairs:
                self.print_five_candles(pair, force=True)

    # ==========================================
    # بوابة الويب المدمجة للهواتف (أندرويد / آيفون) + مراقبة Render 24/7
    # ==========================================
    def render_mobile_connect_html(self, prefill_name: str = "") -> str:
        safe_name = prefill_name.replace('"', "").replace("<", "").replace(">", "")
        token_js = json.dumps(self.web_connect_token)
        base_url_js = json.dumps(PUBLIC_BASE_URL)
        prefill_js = json.dumps(safe_name)

        return f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>ربط حساب عبود تريدينج</title>
<style>
  * {{ box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
  body {{ margin: 0; padding: 14px; background: #0b1120; color: #f8fafc; }}
  .card {{ max-width: 480px; margin: 0 auto; background: #131c31; border: 1px solid #1e293b; border-radius: 18px; padding: 18px; box-shadow: 0 10px 30px rgba(0,0,0,0.4); }}
  .wv-alert {{ display: none; background: #7f1d1d; border: 1px solid #ef4444; color: #fef2f2; padding: 14px; border-radius: 12px; margin-bottom: 16px; font-size: 13px; line-height: 1.6; }}
  h2 {{ margin: 0 0 6px; font-size: 20px; color: #10b981; text-align: center; }}
  p.sub {{ margin: 0 0 16px; font-size: 13px; color: #94a3b8; text-align: center; line-height: 1.5; }}
  .step {{ background: #0f172a; border: 1px solid #1e293b; border-radius: 12px; padding: 14px; margin-bottom: 12px; }}
  .step-title {{ font-weight: 700; font-size: 14px; color: #38bdf8; margin-bottom: 8px; }}
  input, textarea {{ width: 100%; background: #1e293b; border: 1px solid #334155; color: #fff; padding: 11px 12px; border-radius: 10px; font-size: 14px; outline: none; }}
  input:focus, textarea:focus {{ border-color: #10b981; }}
  .btn {{ display: block; width: 100%; text-align: center; padding: 13px; border-radius: 11px; font-weight: 700; font-size: 14px; text-decoration: none; border: none; cursor: pointer; margin-top: 8px; }}
  .btn-primary {{ background: linear-gradient(135deg, #10b981, #059669); color: #fff; }}
  .btn-blue {{ background: linear-gradient(135deg, #2563eb, #1d4ed8); color: #fff; }}
  .btn-amber {{ background: linear-gradient(135deg, #f59e0b, #d97706); color: #0b1120; }}
  .btn-red {{ background: #ef4444; color: #fff; }}
  .hint {{ font-size: 12.5px; color: #cbd5e1; line-height: 1.7; margin-top: 8px; }}
  .badge {{ display: inline-block; background: #064e3b; color: #6ee7b7; padding: 2px 8px; border-radius: 6px; font-size: 11px; }}
  .toast {{ display: none; background: #065f46; color: #ecfdf5; padding: 10px; border-radius: 8px; text-align: center; font-size: 13px; margin-top: 8px; line-height: 1.6; }}
</style>
</head>
<body>
<div class="card">
  <div id="wvBanner" class="wv-alert">
    ⚠️ <b>أنت الآن داخل متصفح تيليجرام الداخلي!</b><br>
    لنجاح ربط الحساب على الهاتف، يجب فتح هذه الصفحة في متصفح <b>Chrome</b> أو <b>Safari</b> الخارجي (مع تشغيل الـ VPN):
    <button class="btn btn-red" onclick="openExternalBrowser()">🌐 فتح في متصفح Chrome / Safari الخارجي</button>
    <button class="btn btn-amber" onclick="copyPageUrl()">📋 نسخ رابط هذه الصفحة لفتحه في المتصفح</button>
  </div>

  <h2>🛡 بوابة ربط حساب Pocket Option</h2>
  <p class="sub">ربط مباشر من الهاتف (أندرويد / آيفون)<br><span class="badge">يحفظ الديمو + الحقيقي + الآيدي معًا</span></p>

  <div class="step">
    <div class="step-title">1️⃣ اسم الحساب (اختياري)</div>
    <input type="text" id="accName" placeholder="مثال: حسابي الرئيسي" value="">
  </div>

  <div class="step">
    <div class="step-title">2️⃣ تجهيز زر الربط في متصفح الهاتف</div>
    <p class="hint">
      • اضغط زر <b>نسخ كود الربط السريع</b> بالأسفل.<br>
      • ثم اضغط قائمة المتصفح (<b>⋮</b> أو المشاركة) ⬅️ <b>إضافة إلى المفضلة (⭐)</b> ⬅️ <b>تعديل (Edit)</b> ⬅️ غيّر الاسم إلى <b>ربط البوت</b> والصق الكود المنسوخ في خانة <b>الرابط (URL)</b>.
    </p>
    <button class="btn btn-amber" onclick="copySmartExtractor()">📋 نسخ كود الربط السريع (Demo + Live)</button>
    <div id="copyToast" class="toast">✅ تم نسخ كود الربط! الآن احفظ هذه الصفحة في المفضلة (⭐) وعدّل الرابط (URL) بلصق الكود فيه.</div>
  </div>

  <div class="step">
    <div class="step-title">3️⃣ تسجيل الدخول وإتمام الربط</div>
    <p class="hint">
      • تأكد أن الـ <b>VPN</b> يعمل، ثم افتح Pocket Option وسجّل الدخول حتى تظهر شموع التداول.<br>
      • بعدها اضغط على شريط عنوان المتصفح، اكتب <b>ربط البوت</b>، واختر المفضلة التي حفظتها ليتم الربط والعودة للبوت فورًا!
    </p>
    <a href="https://pocketoption.com/en/login/" target="_blank" class="btn btn-blue">🌐 فتح تسجيل الدخول في Pocket Option</a>
  </div>

  <div class="step">
    <div class="step-title">🔄 أو الصق كود الجلسة هنا مباشرة</div>
    <form method="POST" action="/api/manual_web_capture">
      <input type="hidden" name="token" value="{self.web_connect_token}">
      <input type="hidden" name="name" id="hiddenFormName" value="">
      <textarea name="raw_session" rows="2" placeholder='الصق هنا 42["auth",...] أو رمز session'></textarea>
      <button type="submit" class="btn btn-primary" onclick="document.getElementById('hiddenFormName').value=document.getElementById('accName').value;">✅ حفظ وتفعيل الحساب فورًا</button>
    </form>
  </div>
</div>

<script>
  const TOKEN = {token_js};
  const BASE_URL = {base_url_js};
  const PREFILL = {prefill_js};
  const nameInput = document.getElementById('accName');
  if (PREFILL) nameInput.value = PREFILL;

  const ua = navigator.userAgent || '';
  const isAndroid = /Android/i.test(ua);
  const isTgWebView = /Telegram/i.test(ua) || (isAndroid && /; wv\)/i.test(ua));
  if (isTgWebView) {{
    document.getElementById('wvBanner').style.display = 'block';
  }}

  function openExternalBrowser() {{
    const cleanUrl = window.location.href.replace(/^https?:\/\//, '');
    if (isAndroid) {{
      window.location.href = 'intent://' + cleanUrl + '#Intent;scheme=https;package=com.android.chrome;end';
    }} else {{
      copyPageUrl();
    }}
  }}

  function copyPageUrl() {{
    const url = window.location.href;
    if (navigator.clipboard && navigator.clipboard.writeText) {{
      navigator.clipboard.writeText(url).then(() => {{
        alert('✅ تم نسخ الرابط! افتح متصفح Safari أو Chrome الآن والصقه في شريط العنوان.');
      }}).catch(() => {{ prompt('انسخ الرابط التالي وافتحه في المتصفح الخارجي:', url); }});
    }} else {{
      prompt('انسخ الرابط التالي وافتحه في المتصفح الخارجي:', url);
    }}
  }}

  function buildExtractorCode() {{
    const nm = encodeURIComponent(nameInput.value.trim() || 'حساب الهاتف');
    const targetPrefix = BASE_URL + '/api/mobile_capture_redirect?token=' + encodeURIComponent(TOKEN) + '&name=' + nm;
    const js = "(async function(){{try{{alert('⏳ جاري ربط الديمو والحقيقي بالبوت...');let h=document.documentElement.innerHTML;let u=(h.match(/[\"']uid[\"']\\s*:\\s*(\\d{{5,15}})/)||[])[1]||'';let ds=(h.match(/[\"']session[\"']\\s*:\\s*[\"']([a-zA-Z0-9_-]{{16,64}})[\"']/)||[])[1]||'';let ls=ds;try{{let r1=await fetch('/en/cabinet/demo-quick-high-low/',{{credentials:'include'}});let t1=await r1.text();let m1=t1.match(/[\"']session[\"']\\s*:\\s*[\"']([a-zA-Z0-9_-]{{16,64}})[\"']/);let u1=t1.match(/[\"']uid[\"']\\s*:\\s*(\\d{{5,15}})/);if(m1)ds=m1[1];if(u1)u=u1[1];}}catch(e){{}}try{{let r2=await fetch('/en/cabinet/quick-high-low/',{{credentials:'include'}});let t2=await r2.text();let m2=t2.match(/[\"']session[\"']\\s*:\\s*[\"']([a-zA-Z0-9_-]{{16,64}})[\"']/);let u2=t2.match(/[\"']uid[\"']\\s*:\\s*(\\d{{5,15}})/);if(m2)ls=m2[1];if(!u&&u2)u=u2[1];}}catch(e){{}}if(!ds&&!ls){{alert('⚠️ تأكد أنك سجلت الدخول وفتحت صفحة التداول أولاً.');return;}}location.href='" + targetPrefix + "&uid='+encodeURIComponent(u)+'&demo_s='+encodeURIComponent(ds)+'&live_s='+encodeURIComponent(ls);}}catch(err){{alert('خطأ: '+err);}}}})();";
    return 'javascript:' + js;
  }}

  function copySmartExtractor() {{
    const code = buildExtractorCode();
    if (navigator.clipboard && navigator.clipboard.writeText) {{
      navigator.clipboard.writeText(code).then(() => {{
        document.getElementById('copyToast').style.display = 'block';
      }}).catch(() => {{ prompt('انسخ هذا الكود بالكامل:', code); }});
    }} else {{
      prompt('انسخ هذا الكود بالكامل:', code);
    }}
  }}
</script>
</body>
</html>"""

    def render_success_html(self, acc: dict) -> str:
        return f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>تم الربط بنجاح</title>
<style>
  body {{ margin: 0; padding: 24px; background: #0b1120; color: #f8fafc; font-family: sans-serif; text-align: center; }}
  .box {{ max-width: 420px; margin: 40px auto; background: #131c31; border: 1px solid #10b981; border-radius: 18px; padding: 26px; }}
  h2 {{ color: #10b981; margin-top: 0; }}
  .info {{ background: #0f172a; padding: 14px; border-radius: 12px; margin: 16px 0; font-size: 14px; line-height: 1.8; }}
  .btn {{ display: inline-block; background: #10b981; color: #fff; padding: 12px 24px; border-radius: 10px; text-decoration: none; font-weight: 700; }}
</style>
</head>
<body>
<div class="box">
  <h2>✅ تم ربط الحساب بنجاح!</h2>
  <div class="info">
    👤 <b>اسم الحساب:</b> {acc['name']}<br>
    🆔 <b>الآيدي (UID):</b> {acc['uid']}<br>
    🧪 <b>الديمو (Demo):</b> جاهز ✅<br>
    💵 <b>الحقيقي (Live):</b> جاهز ✅
  </div>
  <p style="color:#94a3b8;font-size:13px;">تم حفظ الحساب وتفعيله في البوت تلقائيًا. يمكنك الآن العودة إلى تليجرام والتبديل بين الديمو والحقيقي بضغطة زر.</p>
  <a href="tg://resolve" class="btn">🔙 العودة إلى تليجرام</a>
</div>
</body>
</html>"""

    async def start_keepalive_web_server(self):
        async def health_handler(request):
            status = "RUNNING" if self.is_running else "PAUSED"
            return web.Response(text=f"Abood Auto Trader Bot is alive! Status: {status}")

        async def connect_page_handler(request):
            token = request.query.get("token", "")
            if token != self.web_connect_token:
                return web.Response(text="⛔ رابط غير صالح أو منتهي الصلاحية. افتح الرابط من داخل بوت تليجرام.", status=403)
            prefill = request.query.get("name", "")
            return web.Response(text=self.render_mobile_connect_html(prefill), content_type="text/html", charset="utf-8")

        async def mobile_capture_redirect_handler(request):
            token = request.query.get("token", "")
            if token != self.web_connect_token:
                return web.Response(text="⛔ غير مصرح.", status=403)

            name = request.query.get("name", "حساب الهاتف")
            uid = request.query.get("uid", "").strip()
            demo_s = request.query.get("demo_s", "").strip()
            live_s = request.query.get("live_s", "").strip()

            if not demo_s and not live_s:
                return web.Response(text="⚠️ لم يتم استلام بيانات الجلسة.", status=400)

            acc = self.register_captured_account(name, uid or "Unknown", demo_s, live_s)
            self.full_reset_to_scanning("ربط حساب جديد من الهاتف")
            log("MOBILE_AUTH", f"📱 تم ربط الحساب من الهاتف: {acc['name']} (UID: {acc['uid']}) [Demo+Live]")

            asyncio.create_task(self.send_admin(
                f"📱 <b>تم ربط حساب جديد من الهاتف بنجاح!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"👤 <b>الاسم:</b> {acc['name']}\n"
                f"🆔 <b>الآيدي (UID):</b> <code>{acc['uid']}</code>\n"
                f"🧪 <b>الديمو (Demo):</b> جاهز ✅\n"
                f"💵 <b>الحقيقي (Live):</b> جاهز ✅\n\n"
                f"تم تفعيل الحساب تلقائيًا؛ يمكنك التبديل بين التجريبي والحقيقي من القائمة الرئيسية.",
                self.build_main_keyboard()
            ))

            if self.ws:
                asyncio.create_task(self.ws.close())

            return web.Response(text=self.render_success_html(acc), content_type="text/html", charset="utf-8")

        async def manual_web_capture_handler(request):
            data = await request.post()
            token = str(data.get("token", ""))
            if token != self.web_connect_token:
                return web.Response(text="⛔ غير مصرح.", status=403)

            name = str(data.get("name", "حساب الهاتف")).strip() or "حساب الهاتف"
            raw = str(data.get("raw_session", "")).strip()
            parsed = parse_auth_frame(raw)
            if parsed is not None:
                uid = str(parsed.get("uid", "Unknown"))
                sess = str(parsed.get("session", ""))
            else:
                uid = "Unknown"
                sess = raw

            if len(sess) < 10:
                return web.Response(text="⚠️ كود الجلسة غير صحيح. ارجع وحاول مجددًا.", status=400)

            acc = self.register_captured_account(name, uid, sess, sess)
            self.full_reset_to_scanning("ربط حساب من بوابة الهاتف")
            asyncio.create_task(self.send_admin(
                f"✅ <b>تم حفظ الحساب من بوابة الهاتف!</b>\n"
                f"👤 <b>الاسم:</b> {acc['name']} | 🆔 <code>{acc['uid']}</code>",
                self.build_main_keyboard()
            ))
            if self.ws:
                asyncio.create_task(self.ws.close())

            return web.Response(text=self.render_success_html(acc), content_type="text/html", charset="utf-8")

        app = web.Application()
        app.router.add_get("/", health_handler)
        app.router.add_get("/health", health_handler)
        app.router.add_get("/connect", connect_page_handler)
        app.router.add_get("/api/mobile_capture_redirect", mobile_capture_redirect_handler)
        app.router.add_post("/api/manual_web_capture", manual_web_capture_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.getenv("PORT", "10000"))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        log("WEB", f"تم تشغيل خادم المراقبة وبوابة ربط الهاتف على المنفذ {port} ✅")

    async def run(self):
        headers = {
            "Origin": "https://pocketoption.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        await self.start_keepalive_web_server()

        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=connector) as http_session:
            self.http_session = http_session
            asyncio.create_task(self.channel_sender_worker())
            asyncio.create_task(self.telegram_polling_loop())
            asyncio.create_task(self.precision_entry_watcher())
            asyncio.create_task(self.daily_midnight_stats_scheduler())

            await self.send_admin(self.format_dashboard_text(), self.build_main_keyboard())

            ws_params = inspect.signature(websockets.connect).parameters
            header_key = "additional_headers" if "additional_headers" in ws_params else "extra_headers"

            while True:
                self.authenticated = False
                self.subscribed_pairs.clear()
                if self.mode in ("LOCKED", "IN_TRADE"):
                    self.switch_pair_keep_global("إعادة الاتصال بالسيرفر")

                ws_url = self.get_ws_url()
                try:
                    log("WS", f"جاري الاتصال بسيرفر السوق: {ws_url}")
                    connect_kwargs = {
                        header_key: headers,
                        "ping_interval": None,
                        "close_timeout": 5
                    }
                    async with websockets.connect(ws_url, **connect_kwargs) as ws:
                        self.ws = ws
                        log("WS", "تم فتح اتصال WebSocket بنجاح...")
                        hb_task = asyncio.create_task(self.keepalive_heartbeat_loop(ws))
                        scan_task = asyncio.create_task(self.scanner_rotation_loop(ws))

                        async for raw_msg in ws:
                            if isinstance(raw_msg, str):
                                if raw_msg.startswith("0"):
                                    await ws.send("40")
                                elif raw_msg.startswith("40"):
                                    await ws.send(self.auth_frame)
                                elif raw_msg == "2":
                                    await ws.send("3")
                                elif raw_msg.startswith("42"):
                                    try:
                                        parsed = json.loads(raw_msg[2:])
                                        if isinstance(parsed, list) and len(parsed) >= 2:
                                            await self.handle_payload(parsed[0], parsed[1])
                                    except Exception:
                                        pass
                                elif raw_msg.startswith("451-"):
                                    try:
                                        header = json.loads(raw_msg[4:])
                                        if isinstance(header, list) and len(header) >= 1:
                                            self.pending_binary_event = header[0]
                                    except Exception:
                                        self.pending_binary_event = None

                            elif isinstance(raw_msg, bytes):
                                event_name = self.pending_binary_event
                                self.pending_binary_event = None
                                if event_name:
                                    try:
                                        text_data = raw_msg.decode("utf-8", errors="ignore")
                                        indices = [i for i in (text_data.find("{"), text_data.find("[")) if i != -1]
                                        if indices:
                                            json_start = min(indices)
                                            data = json.loads(text_data[json_start:])
                                            await self.handle_payload(event_name, data)
                                    except Exception:
                                        pass

                        hb_task.cancel()
                        scan_task.cancel()
                        self.ws = None
                        log("WS_CLOSE", f"أغلق السيرفر الاتصال (Code: {ws.close_code})")

                except Exception as e:
                    self.ws = None
                    log("ERROR", f"انقطع الاتصال ({e}) — إعادة الاتصال الفورية خلال ثانية...")

                await asyncio.sleep(1.5)


if __name__ == "__main__":
    bot = AboodControlBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log("STOP", "تم إيقاف البوت يدويًا.")
