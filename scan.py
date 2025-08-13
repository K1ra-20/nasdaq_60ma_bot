import os, time, json, requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
# 个人兜底 Chat（没有任何群订阅时就发到你个人，便于确认系统OK）
FALLBACK_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TICKERS = [t.strip().upper() for t in os.environ.get("TICKERS","AAPL,MSFT,GOOG").split(",") if t.strip()]
STATE_UP = Path("last_ups.json")
STATE_DN = Path("last_downs.json")
SUB_FILE = Path("subscribers.json")
OFF_FILE = Path("update_offset.txt")

TG_MAX = 4000  # 给标题/空行留点余量，实际上限约 4096

# ======== 拐点判定参数 ========
SMA_LEN        = 60   # 均线长度（别改）
WINDOW_RECENT  = 15   # 最近连续天数（窗口B长度）
WINDOW_PREVEND = 100   # 窗口A结束位置（相对t）
# 窗口A长度 = WINDOW_PREVEND - WINDOW_RECENT
WINDOW_PREV    = 75
# 窗口A“多数”阈值（默认取过半，向上取整）
THRESHOLD_MAJ  = 72   # (WINDOW_PREV // 2) + 1
# 相对斜率最小幅度（去噪用，0表示不限制；0.0005≈0.05%）
MIN_REL_SLOPE  = 0.0

# 自动计算的检查长度
MIN_DATA_LEN   = SMA_LEN + WINDOW_PREVEND + WINDOW_RECENT
MIN_SLOPE_LEN  = WINDOW_PREVEND + WINDOW_RECENT
# =============================

def load_set(p: Path) -> set:
    try:
        if p.exists():
            import json
            return set(json.loads(p.read_text().strip() or "[]"))
    except Exception:
        pass
    return set()

def save_set(p: Path, s: set):
    import json
    p.write_text(json.dumps(sorted(s)))
    
def chunk_and_send_list_md(chat_id, title, items, new_items: set):
    """将 items（list）按长度分段发送；出现在 new_items 的元素用 **加粗**。"""
    if not items:
        return
    head = title.strip()
    line = ""
    for sym in items:
        token = f"**{sym}**" if sym in new_items else sym
        piece = (", " if line else "") + token
        if len(head) + 1 + len(line) + len(piece) > TG_MAX:
            send_message(chat_id, f"{head}\n{line}", markdown=True)
            time.sleep(0.05)
            line = token
        else:
            line += piece
    if line:
        send_message(chat_id, f"{head}\n{line}", markdown=True)
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
    if (not txt) or "<html" in txt.lower() or txt.lower().startswith("ticker not found"):
        return pd.DataFrame()
    df = pd.read_csv(StringIO(txt))
    df.rename(columns={"Date": "t", "Close": "close"}, inplace=True)
    df["t"] = pd.to_datetime(df["t"], utc=True)
    df = df[["t", "close"]].dropna().sort_values("t")
    if len(df) > 400:
        df = df.iloc[-400:]
    return df

# ---------- 指标与判定 ----------
def detect_turnup(df):
    if df.empty or len(df) < MIN_DATA_LEN:
        return None
    x = df.copy()
    x["sma60"] = x["close"].rolling(SMA_LEN, min_periods=SMA_LEN).mean()
    x["slope"] = x["sma60"].diff()

    if pd.isna(x.iloc[-1]["sma60"]) or pd.isna(x.iloc[-2]["sma60"]):
        return None

    s = x["slope"].dropna()
    if len(s) < MIN_SLOPE_LEN:
        return None

    # 窗口A：t-WINDOW_PREVEND .. t-WINDOW_RECENT
    prev_window = s.iloc[-WINDOW_PREVEND:-WINDOW_RECENT]
    if len(prev_window) < WINDOW_PREV:
        return None
    cond_prev_down = (prev_window <= 0).sum() >= THRESHOLD_MAJ

    # 窗口B：最近连续WINDOW_RECENT天为正
    recentN = s.tail(WINDOW_RECENT)
    cond_recent = (recentN > 0).all()
    if MIN_REL_SLOPE > 0 and cond_recent:
        sma_tail = x["sma60"].dropna().tail(WINDOW_RECENT).values
        rel = recentN.values / sma_tail
        cond_recent = (rel > MIN_REL_SLOPE).all()

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


def detect_turndown(df):
    if df.empty or len(df) < MIN_DATA_LEN:
        return None
    x = df.copy()
    x["sma60"] = x["close"].rolling(SMA_LEN, min_periods=SMA_LEN).mean()
    x["slope"] = x["sma60"].diff()

    if pd.isna(x.iloc[-1]["sma60"]) or pd.isna(x.iloc[-2]["sma60"]):
        return None

    s = x["slope"].dropna()
    if len(s) < MIN_SLOPE_LEN:
        return None

    prev_window = s.iloc[-WINDOW_PREVEND:-WINDOW_RECENT]
    if len(prev_window) < WINDOW_PREV:
        return None
    cond_prev_up = (prev_window >= 0).sum() >= THRESHOLD_MAJ

    recentN = s.tail(WINDOW_RECENT)
    cond_recent = (recentN < 0).all()
    if MIN_REL_SLOPE > 0 and cond_recent:
        sma_tail = x["sma60"].dropna().tail(WINDOW_RECENT).values
        rel = recentN.values / sma_tail
        cond_recent = (rel < -MIN_REL_SLOPE).all()

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

def send_message(chat_id, text, markdown=False):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if markdown:
        payload["parse_mode"] = "Markdown"  # 用 * 和 ** 语法
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
    bad = []
    
    for sym in TICKERS:
        try:
            df = fetch_daily_candles(sym)
            sig_up = detect_turnup(df)
            sig_dn = detect_turndown(df)
            if sig_up: ups.append((sym, sig_up))
            if sig_dn: downs.append((sym, sig_dn))
        except Exception as e:
            bad.append(f"{sym}({e})")
        time.sleep(0.2)

    # 3) 发送结果（聚合清单 + 新增加粗）
    if not recipients:
        return

    # 今天的列表
    up_syms   = sorted(ups)    # 你的 ups/downs 现在是符号列表（上一版我们已这么做）
    down_syms = sorted(downs)

    # 载入昨天的集合
    prev_up   = load_set(STATE_UP)
    prev_down = load_set(STATE_DN)

    # 计算“新增”
    new_up   = set(up_syms)   - prev_up
    new_down = set(down_syms) - prev_down

    if not up_syms and not down_syms:
        for cid in recipients:
            send_message(cid, "✅ 今日无 MA60 趋势拐点（上涨/下跌）。")
            time.sleep(0.05)
    else:
        summary = (
            "🎊 今日 MA60 趋势拐点\n"
            f"📈 由跌转涨: {len(up_syms)} 支 ✨新增 {len(new_up)} \n"
            f"📉 由涨转跌: {len(down_syms)} 支 ✨新增 {len(new_down)} "
        )
        for cid in recipients:
            send_message(cid, summary)
            time.sleep(0.05)
            if up_syms:
                chunk_and_send_list_md(
                    cid,
                    "↗️ 上涨拐点：",
                    up_syms,
                    new_up
                )
            if down_syms:
                chunk_and_send_list_md(
                    cid,
                    "↘️ 下跌拐点：",
                    down_syms,
                    new_down
                )

    # 4) 报告异常标的（可选）
    if bad:
        note = "⚠️ 以下标的数据异常，已跳过：\n" + ", ".join(bad[:50])
        for cid in recipients:
            send_message(cid, note)
            time.sleep(0.05)

    # 5) 保存“今天”的结果，供明日对比
    save_set(STATE_UP, set(up_syms))
    save_set(STATE_DN, set(down_syms))


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
