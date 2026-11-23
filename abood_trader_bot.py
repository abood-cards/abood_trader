import asyncio
import inspect
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
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

# المتغير الثابت للفحص والدخول قبل إغلاق الشمعة/الصفقة بأجزاء الثانية
DEFAULT_LEAD_SECONDS = 0.25

# تعويض فارق أجزاء الثانية بين ساعة Render وساعة سيرفر Pocket Option
SERVER_CLOCK_OFFSET = 0.75

DEFAULT_AUTH_FRAME = os.getenv(
    "PO_AUTH_FRAME",
    '42["auth",{"session":"et97hpjdhses78itun3rk2peld","isDemo":1,"uid":142271281,"platform":2,"isFastHistory":true,"isOptimized":true}]'
)

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


class AboodControlBot:
    def __init__(self):
        # الإعدادات القابلة للتحكم من تليجرام
        self.is_running = False
        self.base_amount = 10.0
        self.global_steps = 6
        self.pair_steps = 4
        self.min_payout = 90
        self.entry_lead_seconds = DEFAULT_LEAD_SECONDS
        self.auth_frame = DEFAULT_AUTH_FRAME

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
        self.demo_balance = None
        self.server_hour_shift = 0.0

        # حالات التداول
        self.mode = "SCANNING"
        self.locked_pair = None
        self.locked_direction = None
        self.locked_target_minute = None

        self.global_step_index = 0
        self.pair_step_index = 0
        self.active_trade_minute = None
        self.active_trade_close_ts = None   # وقت إغلاق الصفقة الفعلي بناءً على لحظة فتحها
        self.trade_entry_price = None       # سعر فتح الصفقة الفعلي من السيرفر

        # سجل الإحصائيات اليومية (Daily Results)
        self.stats_date = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
        self.daily_records = []

    def record_trade_outcome(self, pair_name: str, is_win: bool, attempt_num: int):
        today_str = datetime.now(tz=TZ_UTC3).strftime("%Y-%m-%d")
        if today_str != self.stats_date:
            self.stats_date = today_str
            self.daily_records.clear()

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

        return (
            f"╭━━━[ 🛡 <b>عبود تريدينج</b> ]━━━╮\n"
            f"┃ 📊 <b>DAILY RESULTS — النتائج اليومية</b>\n"
            f"┃ 📅 <code>{date_header}</code>\n"
            f"┃ ⏱ POCKET OPTION • M1\n"
            f"┣━━━━━━━━━━━━━━━━━━━━━━━┫\n"
            f"┃ 🎯 <b>WIN RATE:</b> <b>{win_rate:.1f}%</b>  |  📡 <b>SIGNALS:</b> <b>{total_signals}</b>\n"
            f"┃ 🟢 <b>{win_count} WINS</b>      🔴 <b>{loss_count} LOSSES</b>\n"
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

    def get_trade_ladder(self) -> list[float]:
        ladder = []
        for i in range(self.global_steps):
            val = self.base_amount * (2 ** i)
            ladder.append(int(val) if val.is_integer() else round(val, 2))
        return ladder

    def get_ws_url(self) -> str:
        if '"isDemo":1' in self.auth_frame or '"isDemo": 1' in self.auth_frame:
            return "wss://demo-api-eu.po.market/socket.io/?EIO=4&transport=websocket"
        return "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket"

    def get_server_now(self) -> float:
        return time.time() + self.server_hour_shift

    def switch_pair_keep_global(self, reason: str):
        log("MODE", f"تغيير الزوج مع استمرار التدبيل العام ({self.global_step_index + 1}/{self.global_steps}) — {reason}")
        self.mode = "SCANNING"
        self.locked_pair = None
        self.locked_direction = None
        self.locked_target_minute = None
        self.pair_step_index = 0
        self.active_trade_minute = None
        self.active_trade_close_ts = None
        self.trade_entry_price = None

    def full_reset_to_scanning(self, reason: str):
        log("MODE", f"إعادة ضبط كاملة والعودة للمسح ({reason})")
        self.mode = "SCANNING"
        self.locked_pair = None
        self.locked_direction = None
        self.locked_target_minute = None
        self.global_step_index = 0
        self.pair_step_index = 0
        self.active_trade_minute = None
        self.active_trade_close_ts = None
        self.trade_entry_price = None

    # ==========================================
    # واجهة التحكم (خاص بالأدمن) + الإرسال للقناة
    # ==========================================
    def build_main_keyboard(self) -> dict:
        toggle_btn = (
            {"text": "⏹ إيقاف التداول", "callback_data": "cmd_stop"}
            if self.is_running
            else {"text": "▶️ بدء التداول", "callback_data": "cmd_start"}
        )
        return {
            "inline_keyboard": [
                [toggle_btn, {"text": "📡 تحديث الحالة", "callback_data": "cmd_status"}],
                [{"text": f"🎯 مستوى المسح: {self.get_scope_label()}", "callback_data": "menu_scope"}],
                [
                    {"text": f"💵 الأساسي (${self.base_amount:g})", "callback_data": "set_amount"},
                    {"text": f"📈 العائد (≥ {self.min_payout}%)", "callback_data": "set_payout"}
                ],
                [
                    {"text": f"🌐 التدبيل العام ({self.global_steps})", "callback_data": "set_global_steps"},
                    {"text": f"🏛 تدبيل الزوج ({self.pair_steps})", "callback_data": "set_pair_steps"}
                ],
                [
                    {"text": f"⏱ التقديم ({self.entry_lead_seconds}ث)", "callback_data": "set_lead"},
                    {"text": "🔑 تحديث الجلسة", "callback_data": "set_auth"}
                ],
                [
                    {"text": "📊 معاينة الإحصائيات", "callback_data": "stats_preview"},
                    {"text": "📢 نشر الإحصائيات بالقناة", "callback_data": "stats_publish"}
                ]
            ]
        }

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
        bal_str = f"${self.demo_balance}" if self.demo_balance is not None else "غير متاح"

        active_pairs = self.get_active_scan_pairs()
        eligible_count = sum(1 for p in active_pairs if self.payouts.get(p, 0) >= self.min_payout)

        total_today = len(self.daily_records)
        wins_today = sum(1 for r in self.daily_records if r["result"] == "WIN")
        rate_today = (wins_today / total_today * 100.0) if total_today > 0 else 0.0

        mode_ar = {
            "SCANNING": f"🔍 يبحث عن فرصة (الدخول القادم: ${current_next_amt})",
            "LOCKED": f"🎯 مثبّت على {self.format_signal_symbol(self.locked_pair or '')} (الدخول: ${current_next_amt})",
            "IN_TRADE": (
                f"⚡ داخل صفقة على {self.format_signal_symbol(self.locked_pair or '')} "
                f"[عام {self.global_step_index + 1}/{self.global_steps} | زوج {self.pair_step_index + 1}/{self.pair_steps}]"
            )
        }.get(self.mode, self.mode)

        return (
            f"♟ <b>لوحة إدارة عبود تريدينج (24/7)</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚙️ <b>الحالة:</b> {status_icon}\n"
            f"🌐 <b>السيرفر:</b> {conn_icon}\n"
            f"💰 <b>رصيد الديمو:</b> <code>{bal_str}</code>\n"
            f"📍 <b>النشاط:</b> {mode_ar}\n"
            f"📊 <b>إحصائيات اليوم:</b> <code>{wins_today}/{total_today}</code> ({rate_today:.1f}%)\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🎯 <b>مستوى المسح:</b> <b>{self.get_scope_label()}</b>\n"
            f"📈 <b>الأصول المؤهلة (≥{self.min_payout}%):</b> <code>{eligible_count}</code> من {len(active_pairs)}\n"
            f"💵 <b>المبلغ الأساسي:</b> <code>${self.base_amount:g}</code>\n"
            f"🌐 <b>التدبيل العام ({self.global_steps}):</b> <code>{ladder_str}</code>\n"
            f"🏛 <b>تدبيل الزوج الواحد:</b> <code>{self.pair_steps} محاولات كحد أقصى</code>\n"
            f"⏱ <b>الفحص والدخول قبل الإغلاق:</b> <code>{self.entry_lead_seconds} ثانية</code>\n"
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

    async def send_channel(self, text: str):
        if not CHANNEL_ID or "ضع_" in str(CHANNEL_ID):
            log("WARN", "لم يتم ضبط CHANNEL_ID بعد — تعذّر الإرسال للقناة.")
            return
        payload = {
            "chat_id": CHANNEL_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        res = await self.tg_api("sendMessage", payload, req_timeout=15.0, retries=3)
        if res and res.get("ok"):
            log("CHANNEL", "تم نشر الرسالة في القناة بنجاح ✅")
        else:
            log("CHANNEL_ERR", f"فشل النشر في القناة: {res}")

    async def daily_midnight_stats_scheduler(self):
        while True:
            await asyncio.sleep(20)
            now_dt = datetime.now(tz=TZ_UTC3)
            today_str = now_dt.strftime("%Y-%m-%d")
            if today_str != self.stats_date:
                if self.daily_records:
                    report = self.format_daily_stats_report()
                    await self.send_channel(report)
                    await self.send_admin("📊 تم نشر التقرير اليومي تلقائيًا في القناة وتصفير عداد اليوم الجديد ✅")
                self.stats_date = today_str
                self.daily_records.clear()

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
                            if self.mode == "SCANNING":
                                self.switch_pair_keep_global("تغيير مستوى المسح")
                        elif data == "set_amount":
                            self.awaiting_input = "BASE_AMOUNT"
                            await self.send_admin("💵 أرسل الآن <b>المبلغ الأساسي</b> للصفقة (مثال: <code>10</code>):")
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
                            await self.send_admin("⏱ أرسل الآن <b>الفحص والدخول قبل الإغلاق بأجزاء الثانية</b> (مثال: <code>0.25</code> أو <code>0.15</code>):")
                            continue
                        elif data == "set_auth":
                            self.awaiting_input = "AUTH"
                            await self.send_admin('🔑 أرسل الآن إطار الجلسة الكامل للديمو (<code>42["auth",...]</code>):')
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

                        if self.awaiting_input == "BASE_AMOUNT":
                            try:
                                val = float(text)
                                if val >= 1:
                                    self.base_amount = val
                                    self.awaiting_input = None
                                    await self.send_admin(
                                        f"✅ تم ضبط المبلغ الأساسي إلى <b>${val:g}</b>.\n"
                                        f"🌐 السلم: <code>{' ⬅️ '.join(f'${x}' for x in self.get_trade_ladder())}</code>",
                                        self.build_main_keyboard()
                                    )
                            except ValueError:
                                await self.send_admin("⚠️ أرسل رقمًا صحيحًا (مثال: <code>10</code>):")

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
                                    if self.pair_step_index >= p_steps:
                                        self.pair_step_index = 0
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
                                await self.send_admin("⚠️ أرسل رقمًا عشريًا (مثال: <code>0.25</code>):")

                        elif self.awaiting_input == "AUTH":
                            if text.startswith('42["auth",'):
                                self.auth_frame = text
                                self.awaiting_input = None
                                await self.send_admin("✅ تم تحديث الجلسة بنجاح. جاري إعادة الاتصال...", self.build_main_keyboard())
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
                    self.candles[asset][minute_ts] = {"open": float(row[1]), "close": float(row[2])}
                elif len(row) == 2:
                    ts = normalize_ts(row[0])
                    price = float(row[1])
                    minute_ts = (int(ts) // 60) * 60
                    if minute_ts not in tick_buckets:
                        tick_buckets[minute_ts] = {"open": price, "close": price}
                    else:
                        tick_buckets[minute_ts]["close"] = price
            elif isinstance(row, dict) and "time" in row:
                ts = normalize_ts(row["time"])
                minute_ts = (int(ts) // 60) * 60
                self.candles[asset][minute_ts] = {"open": float(row["open"]), "close": float(row["close"])}

        for m_ts, ohlc in tick_buckets.items():
            if m_ts in self.candles[asset]:
                self.candles[asset][m_ts]["open"] = ohlc["open"]
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
            self.candles[asset][minute_ts] = {"open": price, "close": price}
        else:
            bucket["close"] = price

        if len(self.candles[asset]) > 120:
            oldest_keys = sorted(self.candles[asset].keys())[:-100]
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
    # التنفيذ الفوري، التدبيل المزدوج، والتقييم بسعر الصفقة الفعلي
    # ==========================================
    async def open_trade(self, asset: str, direction: str, g_idx: int, p_idx: int, trade_minute: int, is_draw_retry: bool = False):
        if not self.ws or not self.authenticated or not self.is_running:
            return

        if '"isDemo":1' not in self.auth_frame and '"isDemo": 1' not in self.auth_frame:
            log("SAFETY", "تم إيقاف التنفيذ: الجلسة ليست لحساب تجريبي (isDemo: 1)!")
            self.full_reset_to_scanning("جلسة غير تجريبية")
            return

        ladder = self.get_trade_ladder()
        g_idx = g_idx % len(ladder)
        amount = ladder[g_idx]
        req_id = int(time.time() * 1000)

        self.global_step_index = g_idx
        self.pair_step_index = p_idx
        self.active_trade_minute = trade_minute
        # وقت إغلاق مبدئي من لحظة فتح الصفقة (ويُحدّث فورًا بالوقت الدقيق من successopenOrder)
        self.active_trade_close_ts = self.get_server_now() + TRADE_DURATION
        self.trade_entry_price = self.latest_prices.get(asset)
        self.mode = "IN_TRADE"

        order_payload = {
            "asset": asset,
            "amount": amount,
            "action": direction,
            "isDemo": 1,
            "requestId": req_id,
            "optionType": 100,
            "time": TRADE_DURATION
        }

        await self.ws.send(f'42["openOrder",{json.dumps(order_payload)}]')

        sig_sym = self.format_signal_symbol(asset)
        dir_text = "🟢 CALL UP 📈" if direction == "call" else "🔴 PUT DOWN 📉"
        tag_note = " (إعادة بعد تعادل ⚖️)" if is_draw_retry else ""
        log(
            "TRADE_EXEC",
            f"⚡ تنفيذ{tag_note} | {sig_sym} | {dir_text} | المبلغ: ${amount} | "
            f"سعر الدخول المبدئي: {self.trade_entry_price} | [عام {g_idx + 1}/{self.global_steps} | زوج {p_idx + 1}/{self.pair_steps}]"
        )

        if p_idx == 0 and not is_draw_retry:
            confirm_msg = (
                f"🚀 <b>تم تأكيد الدخول في الصفقة ✅</b>\n"
                f"🏛 <code>{sig_sym}</code> | {dir_text}"
            )
            asyncio.create_task(self.send_channel(confirm_msg))

    def evaluate_active_trade_result(self, asset: str, direction: str) -> str:
        """
        تقييم الصفقة مقارنة بسعر فتح الصفقة الفعلي (openPrice) وليس بافتتاح الشمعة.
        يرجع: "WIN" | "LOSS" | "DRAW"
        """
        current_price = self.latest_prices.get(asset)
        entry_price = self.trade_entry_price

        if current_price is not None and entry_price is not None:
            if current_price == entry_price:
                return "DRAW"
            if direction == "put":
                return "WIN" if current_price < entry_price else "LOSS"
            else:
                return "WIN" if current_price > entry_price else "LOSS"

        # احتياطي فقط في حال لم يتوفر السعر الحي
        candle = self.candles.get(asset, {}).get(self.active_trade_minute)
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
            if not self.is_running or self.mode == "SCANNING" or not self.locked_pair:
                await asyncio.sleep(0.05)
                continue

            server_now = self.get_server_now()

            # 1) فحص الشمعة الخامسة في آخر أجزاء من الثانية للدخول الأساسي
            if self.mode == "LOCKED" and self.locked_target_minute:
                candle_close_ts = self.locked_target_minute + 60.0 + SERVER_CLOCK_OFFSET
                remaining = candle_close_ts - server_now

                if remaining > 2.0:
                    await asyncio.sleep(0.05)
                    continue
                elif remaining > self.entry_lead_seconds:
                    await asyncio.sleep(0.005)
                    continue

                asset = self.locked_pair
                sig_sym = self.format_signal_symbol(asset)
                eligible, payout = self.is_pair_eligible(asset)
                if not eligible:
                    log("SKIP_PAYOUT", f"انخفض عائد {asset} إلى {payout}% — إلغاء الإشارة")
                    asyncio.create_task(self.send_channel(
                        f"⚪ <b>تم إلغاء الإشارة</b>\n"
                        f"🏛 <code>{sig_sym}</code>"
                    ))
                    self.switch_pair_keep_global("انخفاض نسبة العائد")
                    continue

                target_candle = self.candles.get(asset, {}).get(self.locked_target_minute)
                if not target_candle:
                    self.switch_pair_keep_global("فقدان بيانات الشمعة")
                    continue

                live_color = candle_color(target_candle["open"], target_candle["close"])
                next_trade_minute = self.locked_target_minute + 60
                log(
                    "PRE_CLOSE",
                    f"⏱ فحص قبل الإغلاق بـ {max(0.0, remaining):.3f}ث (المضبوط: {self.entry_lead_seconds}ث) | {asset} | {color_label(live_color)}"
                )

                if self.locked_direction == "put":
                    if live_color == "GREEN":
                        await self.open_trade(
                            asset, "put",
                            g_idx=self.global_step_index,
                            p_idx=0,
                            trade_minute=next_trade_minute
                        )
                    else:
                        asyncio.create_task(self.send_channel(
                            f"⚪ <b>تم إلغاء الإشارة</b>\n"
                            f"🏛 <code>{sig_sym}</code>"
                        ))
                        self.switch_pair_keep_global("أغلقت حمراء عكس الشرط")

                elif self.locked_direction == "call":
                    if live_color == "RED":
                        await self.open_trade(
                            asset, "call",
                            g_idx=self.global_step_index,
                            p_idx=0,
                            trade_minute=next_trade_minute
                        )
                    else:
                        asyncio.create_task(self.send_channel(
                            f"⚪ <b>تم إلغاء الإشارة</b>\n"
                            f"🏛 <code>{sig_sym}</code>"
                        ))
                        self.switch_pair_keep_global("أغلقت خضراء عكس الشرط")

            # 2) فحص الصفقة المفتوحة بناءً على وقت إغلاق الصفقة الفعلي وسعر دخولها الفعلي
            elif self.mode == "IN_TRADE" and self.active_trade_close_ts:
                remaining = self.active_trade_close_ts - server_now

                if remaining > 2.0:
                    await asyncio.sleep(0.05)
                    continue
                elif remaining > self.entry_lead_seconds:
                    await asyncio.sleep(0.005)
                    continue

                asset = self.locked_pair
                sig_sym = self.format_signal_symbol(asset)
                direction = self.locked_direction
                result = self.evaluate_active_trade_result(asset, direction)

                attempt_num = self.global_step_index + 1
                sup_num = to_superscript(attempt_num)
                curr_p = self.latest_prices.get(asset)

                log(
                    "TRADE_CHECK",
                    f"⏱ فحص الصفقة قبل الإغلاق بـ {max(0.0, remaining):.3f}ث | {asset} | "
                    f"دخول: {self.trade_entry_price} vs حالي: {curr_p} => {result} "
                    f"[عام {attempt_num}/{self.global_steps} | زوج {self.pair_step_index + 1}/{self.pair_steps}]"
                )

                if result == "WIN":
                    self.record_trade_outcome(sig_sym, is_win=True, attempt_num=attempt_num)
                    win_msg = (
                        f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                        f"┃ 🏛 <code>{sig_sym}</code>\n"
                        f"┃ 🏆 <b>WIN{sup_num} ✅</b>\n"
                        f"╰━━━━━━━━━━━━━━━╯"
                    )
                    asyncio.create_task(self.send_channel(win_msg))
                    self.full_reset_to_scanning(f"صفقة رابحة WIN{attempt_num}")
                    continue

                next_minute = (self.active_trade_minute or int(server_now // 60) * 60) + 60

                # حالة التعادل (DRAW): إعادة الدخول بنفس المبلغ بدون تدبيل
                if result == "DRAW":
                    log("DRAW", f"⚖️ تعادل على {sig_sym} — إعادة الدخول بنفس المبلغ دون تدبيل.")
                    await self.open_trade(
                        asset,
                        direction,
                        g_idx=self.global_step_index,
                        p_idx=self.pair_step_index,
                        trade_minute=next_minute,
                        is_draw_retry=True
                    )
                    continue

                # حالة الخسارة (LOSS): تطبيق التدبيل المزدوج
                next_g_idx = self.global_step_index + 1
                next_p_idx = self.pair_step_index + 1

                if next_g_idx >= self.global_steps:
                    self.record_trade_outcome(sig_sym, is_win=False, attempt_num=attempt_num)
                    loss_msg = (
                        f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                        f"┃ 🏛 <code>{sig_sym}</code>\n"
                        f"┃ ❌ <b>LOSS</b>\n"
                        f"╰━━━━━━━━━━━━━━━╯"
                    )
                    asyncio.create_task(self.send_channel(loss_msg))
                    next_g_idx = 0

                    if next_p_idx < self.pair_steps:
                        await self.open_trade(asset, direction, g_idx=next_g_idx, p_idx=next_p_idx, trade_minute=next_minute)
                    else:
                        self.full_reset_to_scanning("انتهاء التدبيل العام واستنفاد محاولات الزوج")
                else:
                    if next_p_idx < self.pair_steps:
                        await self.open_trade(asset, direction, g_idx=next_g_idx, p_idx=next_p_idx, trade_minute=next_minute)
                    else:
                        self.global_step_index = next_g_idx
                        self.switch_pair_keep_global(
                            f"اكتمال {self.pair_steps} محاولات على {sig_sym}"
                        )

    async def process_market_logic(self, asset: str, force_print: bool = False):
        if not self.is_running:
            return

        if asset not in self.get_active_scan_pairs() and asset != self.locked_pair:
            return

        eligible, payout = self.is_pair_eligible(asset)
        if not eligible:
            return

        self.print_five_candles(asset, force=force_print)

        if self.mode == "SCANNING":
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

            # شرط البيع: أحمر -> أحمر -> أخضر -> أحمر
            if c4 == "RED" and c3 == "RED" and c2 == "GREEN" and c1 == "RED":
                self.mode = "LOCKED"
                self.locked_pair = asset
                self.locked_direction = "put"
                self.locked_target_minute = m_open
                self.pair_step_index = 0
                log("LOCK", f"🎯 رصد نموذج بيع على {sig_sym} ({payout}%) | الدخول: {entry_time_str} [UTC+3]")

                signal_msg = (
                    f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                    f"┃ 🏛 <code>{sig_sym}</code>\n"
                    f"┃ ⏰ Entry  <code>{entry_time_str}</code> [UTC+3]\n"
                    f"┃ ⏱ Expiry  M1\n"
                    f"┃ 🔴 <b>PUT DOWN 📉</b>\n"
                    f"┃ 📌 إذا أغلقت خضراء 🟢 ⬅️ بيع\n"
                    f"┃ ⚠️ إذا أغلقت حمراء 🔴 ⬅️ لا تدخل\n"
                    f"╰━━━━━━━━━━━━━━━╯"
                )
                asyncio.create_task(self.send_channel(signal_msg))

            # شرط الشراء: أخضر -> أخضر -> أحمر -> أخضر
            elif c4 == "GREEN" and c3 == "GREEN" and c2 == "RED" and c1 == "GREEN":
                self.mode = "LOCKED"
                self.locked_pair = asset
                self.locked_direction = "call"
                self.locked_target_minute = m_open
                self.pair_step_index = 0
                log("LOCK", f"🎯 رصد نموذج شراء على {sig_sym} ({payout}%) | الدخول: {entry_time_str} [UTC+3]")

                signal_msg = (
                    f"╭━━━[ 🛡 عبود تريدينج ]━╮\n"
                    f"┃ 🏛 <code>{sig_sym}</code>\n"
                    f"┃ ⏰ Entry  <code>{entry_time_str}</code> [UTC+3]\n"
                    f"┃ ⏱ Expiry  M1\n"
                    f"┃ 🟢 <b>CALL UP 📈</b>\n"
                    f"┃ 📌 إذا أغلقت حمراء 🔴 ⬅️ شراء\n"
                    f"┃ ⚠️ إذا أغلقت خضراء 🟢 ⬅️ لا تدخل\n"
                    f"╰━━━━━━━━━━━━━━━╯"
                )
                asyncio.create_task(self.send_channel(signal_msg))

    async def handle_payload(self, event_name: str, data):
        if event_name == "successauth":
            self.authenticated = True
            log("AUTH", "تم تأكيد المصادقة مع سيرفر Pocket Option بنجاح ✅")
            return

        if event_name == "updateAssets" and isinstance(data, list):
            self.update_assets_payouts(data)
            return

        if event_name == "successupdateBalance" and isinstance(data, dict):
            self.demo_balance = data.get("balance")
            log("BALANCE", f"رصيد حساب الديمو الحالي: ${self.demo_balance}")
            return

        if event_name == "successopenOrder" and isinstance(data, dict):
            # التقاط سعر فتح الصفقة الفعلي ووقت إغلاقها الفعلي من السيرفر مباشرة
            if data.get("openPrice") is not None:
                self.trade_entry_price = float(data["openPrice"])
            if data.get("closeTimestamp") is not None:
                self.active_trade_close_ts = normalize_ts(data["closeTimestamp"])
            elif data.get("openTimestamp") is not None:
                self.active_trade_close_ts = normalize_ts(data["openTimestamp"]) + TRADE_DURATION

            log(
                "ORDER_OK",
                f"تأكيد السيرفر | ID: {data.get('id')} | المبلغ: ${data.get('amount')} | "
                f"سعر الفتح الفعلي: {self.trade_entry_price}"
            )
            return

        if event_name == "updateStream":
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, list) and len(item) >= 3:
                        asset, ts, price = str(item[0]), item[1], float(item[2])
                        self.update_candle_from_tick(asset, ts, price)
                        if self.mode == "SCANNING" or asset == self.locked_pair:
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
        while True:
            if not self.is_running:
                await asyncio.sleep(1)
                continue

            if self.mode in ("LOCKED", "IN_TRADE") and self.locked_pair:
                pair = self.locked_pair
                self.active_symbol = pair
                try:
                    await ws.send(f'42["changeSymbol",{{"asset":"{pair}","period":60}}]')
                except Exception:
                    break
                await asyncio.sleep(2)
                continue

            active_pairs = self.get_active_scan_pairs()
            if not active_pairs:
                await asyncio.sleep(1)
                continue

            pair = active_pairs[idx % len(active_pairs)]
            idx += 1

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
                    "offset": 900,
                    "period": 60
                }
                await ws.send(f'42["loadHistoryPeriod",{json.dumps(history_req)}]')
            except Exception:
                break

            await asyncio.sleep(SCAN_INTERVAL_PER_PAIR)
            if self.mode == "SCANNING" and self.is_running:
                self.print_five_candles(pair, force=True)

    # ==========================================
    # خادم الويب المدمج لإبقاء الاستضافة المجانية على Render نشطة 24/7
    # ==========================================
    async def start_keepalive_web_server(self):
        async def health_handler(request):
            status = "RUNNING" if self.is_running else "PAUSED"
            return web.Response(text=f"Abood Auto Trader Bot is alive! Status: {status}")

        app = web.Application()
        app.router.add_get("/", health_handler)
        app.router.add_get("/health", health_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        port = int(os.getenv("PORT", "10000"))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        log("WEB", f"تم تشغيل خادم المراقبة على المنفذ {port} لاستضافة Render 24/7 ✅")

    async def run(self):
        headers = {
            "Origin": "https://pocketoption.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        await self.start_keepalive_web_server()

        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=connector) as http_session:
            self.http_session = http_session
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
