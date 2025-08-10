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

TG_MAX = 4000  # 给标题/空行留点余量，实际上限约 4096

def chunk_and_send_list(chat_id, title, items):
    """
    将 items（列表/集合）按逗号+空格拼接，并在不超过 TG_MAX 的前提下分多条消息发送。
    """
    if not items:
        return
    head = title.strip()
    line = ""
    for sym in items:
        piece = (", " if line else "") + sym
        # 如果再加就会超长，先发一条
        if len(head) + 1 + len(line) + len(piece) > TG_MAX:
            text = f"{head}\n{line}"
            send_message(chat_id, text)
            time.sleep(0.05)
            line = sym  # 新的一段以当前 symbol 开头
        else:
            line += piece
    # 发送剩余部分
    if line:
        text = f"{head}\n{line}"
        send_message(chat_id, text)
        time.sleep(0.05)


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
def detect_turnup(df, min_rel_slope=0.0):
    """
    稳健向上拐点：
      - 窗口 A：t-40..t-10 的 30 天里，SMA60 日斜率多数(>=16) <= 0
      - 窗口 B：最近 10 天，SMA60 日斜率连续 > 0
      - 可选：每一天相对斜率 > min_rel_slope（默认0）
    返回 dict 或 None
    """
    if df.empty or len(df) < 110:  # 给足SMA60与窗口长度
        return None
    x = df.copy()
    x["sma60"] = x["close"].rolling(60, min_periods=60).mean()
    x["slope"] = x["sma60"].diff()

    # 最近一日索引
    if pd.isna(x.iloc[-1]["sma60"]) or pd.isna(x.iloc[-2]["sma60"]):
        return None

    # 取出斜率序列（去掉前面 NaN）
    s = x["slope"].dropna()
    if len(s) < 50:  # 至少能覆盖 -40..-10 与最近10天
        return None

    # 窗口 A：t-40..t-10 （不含今天的 10 天）
    prev_window = s.iloc[-(10+40):-10]  # 长度应为30
    if len(prev_window) < 30:
        return None
    cond_prev_down = (prev_window <= 0).sum() >= 16

    # 窗口 B：最近10天全为正
    recent10 = s.tail(10)
    if (recent10 > 0).all():
        if min_rel_slope > 0:
            # 每天的相对斜率都需超过阈值
            sma_tail = x["sma60"].dropna().tail(10).values
            rel = recent10.values / sma_tail
            cond_recent = (rel > min_rel_slope).all()
        else:
            cond_recent = True
    else:
        cond_recent = False

    if cond_prev_down and cond_recent:
        last = x.iloc[-1]
        return {
            "date": last["t"].date(),
            "close": float(last["close"]),
            "sma60": float(last["sma60"]),
            "slope": float(last["slope"]),
            "type": "up",
        }
    return None


def detect_turndown(df, min_rel_slope=0.0):
    """
    稳健向下拐点（完全反向）：
      - 窗口 A：t-40..t-10 的 30 天里，SMA60 日斜率多数(>=16) >= 0
      - 窗口 B：最近 10 天，SMA60 日斜率连续 < 0
      - 可选：每一天相对斜率 < -min_rel_slope（默认0）
    """
    if df.empty or len(df) < 110:
        return None
    x = df.copy()
    x["sma60"] = x["close"].rolling(60, min_periods=60).mean()
    x["slope"] = x["sma60"].diff()

    if pd.isna(x.iloc[-1]["sma60"]) or pd.isna(x.iloc[-2]["sma60"]):
        return None

    s = x["slope"].dropna()
    if len(s) < 50:
        return None

    prev_window = s.iloc[-(10+40):-10]
    if len(prev_window) < 30:
        return None
    cond_prev_up = (prev_window >= 0).sum() >= 16

    recent10 = s.tail(10)
    if (recent10 < 0).all():
        if min_rel_slope > 0:
            sma_tail = x["sma60"].dropna().tail(10).values
            rel = recent10.values / sma_tail
            cond_recent = (rel < -min_rel_slope).all()
        else:
            cond_recent = True
    else:
        cond_recent = False

    if cond_prev_up and cond_recent:
        last = x.iloc[-1]
        return {
            "date": last["t"].date(),
            "close": float(last["close"]),
            "sma60": float(last["sma60"]),
            "slope": float(last["slope"]),
            "type": "down",
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

    # 2) 扫描（同时找上涨/下跌拐点）
    ups, downs = [], []
    for sym in TICKERS:
        df = fetch_daily_candles(sym)
        sig_up = detect_turnup(df, min_rel_slope=0.0)     # 如需更稳，把 0.0 调成 0.0002
        sig_dn = detect_turndown(df, min_rel_slope=0.0)  # 同上
        if sig_up:  ups.append((sym, sig_up))
        if sig_dn:  downs.append((sym, sig_dn))
        time.sleep(0.2)  # 适度节流，Stooq 没严格限速

    # 3) 发送（聚合成清单，只报代码）
    if not recipients:
        return

    up_symbols   = [sym for sym, _ in ups]
    down_symbols = [sym for sym, _ in downs]

    if not up_symbols and not down_symbols:
        for cid in recipients:
            send_message(cid, "✅ 今日无 MA60 趋势拐点（上涨/下跌）。")
            time.sleep(0.05)
        return

    # 先发一个总览（数量统计）
    summary = f"📊 今日 MA60 趋势拐点\n" \
              f"↗️ 上涨拐点: {len(up_symbols)} 支\n" \
              f"↘️ 下跌拐点: {len(down_symbols)} 支"
    for cid in recipients:
        send_message(cid, summary)
        time.sleep(0.05)

        if up_symbols:
            chunk_and_send_list(cid, "↗️ 上涨拐点：", sorted(up_symbols))
        if down_symbols:
            chunk_and_send_list(cid, "↘️ 下跌拐点：", sorted(down_symbols))

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
