import os, time, json, requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
# 个人兜底 Chat（没有任何群订阅时就发到你个人，便于确认系统OK）
FALLBACK_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TICKERS = [t.strip().upper() for t in os.environ.get("TICKERS","AAPL,MSFT,GOOG").split(",") if t.strip()]

SUB_FILE = Path("subscribers.json")
OFF_FILE = Path("update_offset.txt")

# ---------- 数据抓取：Stooq EOD ----------
def fetch_daily_candles(symbol):
    s = symbol.lower()
    if "." not in s:
        s = f"{s}.us"   # 美股后缀
    url = f"https://stooq.com/q/d/l/?s={s}&i=d"
    r = requests.get(url, timeout=20, headers={"User-Agent": "ma60-telegram-bot/1.0"})
    r.raise_for_status()
    txt = r.text.strip()
    if (not txt) or txt.lower().startswith("ticker not found"):
        return pd.DataFrame()
    df = pd.read_csv(StringIO(txt))
    df.rename(columns={"Date": "t", "Close": "close"}, inplace=True)
    df["t"] = pd.to_datetime(df["t"], utc=True)
    df = df[["t", "close"]].dropna().sort_values("t")
    if len(df) > 400:
        df = df.iloc[-400:]
    return df

# ---------- 指标与判定 ----------
def detect_turnup(df, eps=0.0005):
    if df.empty:
        return None
    df = df.copy()
    df["sma60"] = df["close"].rolling(60, min_periods=60).mean()
    df["slope"] = df["sma60"].diff()
    if len(df) < 61 or pd.isna(df.iloc[-1]["sma60"]) or pd.isna(df.iloc[-2]["sma60"]):
        return None
    row_t  = df.iloc[-1]
    row_t1 = df.iloc[-2]
    if (row_t1["slope"] <= 0) and (row_t["slope"] > 0) and (row_t["slope"]/row_t["sma60"] > eps):
        return {
            "date": row_t["t"].date(),
            "close": float(row_t["close"]),
            "sma60": float(row_t["sma60"]),
            "slope": float(row_t["slope"]),
        }
    return None

# ---------- Telegram 基础 ----------
def tg_get(url_path, params=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{url_path}"
    r = requests.get(url, params=params or {}, timeout=20)
    r.raise_for_status()
    return r.json()

def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    requests.post(url, json=payload, timeout=20)

# ---------- 订阅管理：从 getUpdates 自动同步 ----------
def load_subscribers():
    if SUB_FILE.exists():
        try:
            return set(json.loads(SUB_FILE.read_text().strip() or "[]"))
        except Exception:
            return set()
    return set()

def save_subscribers(subs:set):
    SUB_FILE.write_text(json.dumps(sorted(int(x) for x in subs), ensure_ascii=False, indent=0))

def load_offset():
    if OFF_FILE.exists():
        try:
            return int(OFF_FILE.read_text().strip() or "0")
        except Exception:
            return 0
    return 0

def save_offset(v:int):
    OFF_FILE.write_text(str(int(v)))

def sync_subscribers_from_updates():
    """
    把当天新增/退出的群自动写入 subscribers.json
    规则：
      - my_chat_member: 机器人被加入（member/administrator/creator）=> 订阅；被踢/退出（left/kicked）=> 取消
      - message: 群里有人发 /subscribe => 订阅；/unsubscribe => 取消
    注意：Telegram 只保存 24h 内未拉取的更新，所以把机器人拉入群后当天在群里随便发一句 /subscribe 更保险
    """
    subs = load_subscribers()
    offset = load_offset()
    max_id = offset

    params = {
        "offset": offset + 1,
        "timeout": 0,
        "allowed_updates": '["my_chat_member","message"]'
    }
    try:
        data = tg_get("getUpdates", params)
    except Exception:
        # 拉取失败就保持现状
        return subs

    if not data.get("ok"):
        return subs

    for upd in data.get("result", []):
        uid = upd.get("update_id", 0)
        if uid > max_id:
            max_id = uid

        mc = upd.get("my_chat_member")
        if mc:
            chat = mc.get("chat", {})
            cid  = chat.get("id")
            new_status = (mc.get("new_chat_member") or {}).get("status", "")
            if cid:
                if new_status in ("member","administrator","creator"):
                    subs.add(int(cid))
                elif new_status in ("left","kicked"):
                    subs.discard(int(cid))
            continue

        msg = upd.get("message")
        if msg:
            chat = msg.get("chat", {})
            ctype = chat.get("type")
            cid   = chat.get("id")
            text  = (msg.get("text") or "").lower()
            if ctype in ("group","supergroup") and cid:
                if "/subscribe" in text:
                    subs.add(int(cid))
                if "/unsubscribe" in text:
                    subs.discard(int(cid))

    # 保存 offset & 订阅表
    save_offset(max_id)
    save_subscribers(subs)
    return subs

# ---------- 主流程 ----------
def main():
    # 1) 同步订阅
    subscribers = sync_subscribers_from_updates()

    # 如果没有任何群订阅，就用个人兜底，确保你能看到结果
    recipients = set(subscribers)
    if not recipients and FALLBACK_CHAT_ID:
        try:
            recipients.add(int(FALLBACK_CHAT_ID))
        except Exception:
            pass

    # 2) 扫描
    signals = []
    for sym in TICKERS:
        df = fetch_daily_candles(sym)
        sig = detect_turnup(df)
        if sig:
            signals.append((sym, sig))
        time.sleep(0.2)  # 适度节流，Stooq 没严格限速

    # 3) 发送
    if not recipients:
        # 没有任何可发对象就算了（避免报错）
        return

    if not signals:
        for cid in recipients:
            send_message(cid, "✅ 今日无 MA60 由降转升的标的。")
            time.sleep(0.05)
        return

    for sym, s in signals:
        text = (
            f"📈 {sym} MA60 由降转升\n"
            f"日期: {s['date']}\n"
            f"收盘: {s['close']:.2f}  SMA60: {s['sma60']:.2f}  Δ:{s['slope']:.4f}\n"
            f"图表: https://www.tradingview.com/symbols/{sym}/"
        )
        for cid in recipients:
            send_message(cid, text)
            time.sleep(0.05)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 报错时尽量通知你
        if FALLBACK_CHAT_ID:
            try:
                send_message(FALLBACK_CHAT_ID, f"❌ 运行失败：{e}")
            except Exception:
                pass
        raise
