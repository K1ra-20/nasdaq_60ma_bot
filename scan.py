import os, time, json, requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
# 个人兜底 Chat（没有任何群订阅时就发到你个人，便于确认系统OK）
FALLBACK_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
TICKERS = [t.strip().upper() for t in os.environ.get("TICKERS","AAPL,MSFT,GOOG").split(",") if t.strip()]
USE_SP500 = os.environ.get("USE_SP500", "true").lower() == "true" # 开关：是否使用自定义股票清单

SUB_FILE = Path("subscribers.json")
OFF_FILE = Path("update_offset.txt")
LAST_FILE = Path("last_signals.json")

TG_MAX = 4000  # 给标题/空行留点余量，实际上限约 4096

# ======== 拐点判定参数 ========
SMA_LEN        = 60   # 均线长度（60，别改）
WINDOW_RECENT  = 12   # 最近连续天数（窗口B长度）
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
def fetch_sp500_tickers():
    """
    动态抓取 S&P 500 成分股代码（Symbol 列），返回大写的去重列表。
    - 首选：维基百科“List of S&P 500 companies”页面的第一张表
    - 备用：NASDAQ 的成分页（若维基失败）
    - 失败时：返回空列表（主流程会提示）
    """
    headers = {"User-Agent": "ma60-telegram-bot/1.0 (+github-actions)"}

    # 1) 维基百科
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        html = requests.get(url, timeout=30, headers=headers).text
        tables = pd.read_html(html)  # 需要 lxml
        # 通常第一张表就是 constituents
        for tbl in tables:
            cols = [c.lower() for c in tbl.columns]
            if any("symbol" in c for c in cols):
                symcol = tbl.columns[[i for i,c in enumerate(cols) if "symbol" in c][0]]
                syms = (
                    tbl[symcol]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                    .tolist()
                )
                # 清理特殊符号（去掉空、非字母数字/点/破折号）
                cleaned = []
                for s in syms:
                    s = s.replace("\u200b", "").replace(" ", "")
                    if s and all(ch.isalnum() or ch in {".", "-"} for ch in s):
                        cleaned.append(s)
                if cleaned:
                    return sorted(set(cleaned))
    except Exception:
        pass

    # 2) 备用来源（NASDAQ 指数成分接口/页面常有反爬；这里留作兜底示例）
    try:
        url = "https://www.nasdaq.com/market-activity/quotes/s-and-p-500"
        html = requests.get(url, timeout=30, headers=headers).text
        # 有站点防爬时这里可能拿不到完整列表；简单正则兜底
        import re
        guess = re.findall(r'"/market-activity/stocks/([A-Za-z0-9\.-]{1,10})"', html)
        if guess:
            syms = [g.upper() for g in guess]
            return sorted(set(syms))
    except Exception:
        pass

    return []

def _extract_symbol(x):
    """从多种历史形态里提取股票代码为字符串。支持 str / (sym, ...) / dict"""
    if isinstance(x, str):
        return x.strip().upper()
    if isinstance(x, (list, tuple)) and len(x) >= 1:
        return str(x[0]).strip().upper()
    if isinstance(x, dict):
        for k in ("symbol", "sym", "ticker"):
            if k in x and x[k]:
                return str(x[k]).strip().upper()
    return None

def only_symbols(items):
    """把列表里的元素统一转换成纯股票代码字符串列表，过滤掉 None。"""
    out = []
    for it in items:
        sym = _extract_symbol(it)
        if sym:
            out.append(sym)
    return out

def load_last_signals():
    """读取昨天播报过的代码集合（上下拐点各一组），并做兼容清洗。"""
    if not LAST_FILE.exists():
        return set(), set()
    try:
        raw = LAST_FILE.read_text().strip() or "{}"
        data = json.loads(raw)
        up_raw = data.get("ups", [])
        dn_raw = data.get("downs", [])
        up = set(only_symbols(up_raw))
        dn = set(only_symbols(dn_raw))
        return up, dn
    except Exception:
        return set(), set()

def save_last_signals(up_set: set, dn_set: set):
    """只保存字符串代码，避免下次读到 dict/tuple。"""
    data = {
        "ups": sorted([_extract_symbol(x) for x in up_set if _extract_symbol(x)]),
        "downs": sorted([_extract_symbol(x) for x in dn_set if _extract_symbol(x)]),
    }
    LAST_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=0))
    
def chunk_and_send_list(chat_id, title, items, highlight:set=None):
    """
    将 items 按逗号分隔拼接并分段发送。
    highlight: 需要加粗的代码集合（例如今天的新出现的）。
    """
    highlight = highlight or set()
    if not items:
        return
    head = title.strip()
    line = ""
    for sym in items:
        label = f"**{sym}**" if sym in highlight else sym
        piece = (", " if line else "") + label
        if len(head) + 1 + len(line) + len(piece) > TG_MAX:
            send_message(chat_id, f"{head}\n{line}")
            time.sleep(0.05)
            line = label
        else:
            line += piece
    if line:
        send_message(chat_id, f"{head}\n{line}")
        time.sleep(0.05)


# ---------- 数据抓取：Stooq EOD ----------
def _to_stooq_symbol(symbol: str) -> str:
    """
    把标准美股代码转成 Stooq 查询用代码：
    - 小写
    - 默认追加 '.us' 后缀
    - 保留点号（BRK.B, BF.B 等）
    """
    s = (symbol or "").strip().lower()
    # 常见：stooq 支持 'brk.b.us' 这种写法；不替换成破折号，保留点号更稳
    if "." not in s:
        s = f"{s}.us"
    elif not s.endswith(".us"):
        s = f"{s}.us"
    return s

def fetch_daily_candles(symbol):
    stooq_sym = _to_stooq_symbol(symbol)
    url = f"https://stooq.com/q/d/l/?s={stooq_sym}&i=d"
    r = requests.get(url, timeout=20, headers={"User-Agent": "ma60-telegram-bot/1.0"})
    txt = r.text.strip()
    if (not txt) or "<html" in txt.lower() or txt.lower().startswith("ticker not found"):
        return pd.DataFrame()
    df = pd.read_csv(StringIO(txt))
    df.rename(columns={"Date": "t", "Close": "close"}, inplace=True)
    df["t"] = pd.to_datetime(df["t"], utc=True, errors="coerce")
    df = df[["t", "close"]].dropna().sort_values("t")
    if len(df) > 400:
        df = df.iloc[-400:]
    df = df.set_index("t")
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

def send_message(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "Markdown",  # 让 **粗体** 生效（用经典 Markdown，免去V2的转义麻烦）
    }
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
    # 0) 如果开启 S&P 500 模式，动态拉取成分股作为 TICKERS
    global TICKERS
    if USE_SP500:
        sp = fetch_sp500_tickers()
        if sp:
            TICKERS = sp
        else:
            # 拉取失败时，保底用环境变量/Secrets 里的 TICKERS
            pass
            
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

    # 3) 生成并发送（聚合清单 + 新增加粗）
    if not recipients:
        return

    # ups / downs 上面循环里建议直接 append(sym)
    up_symbols   = sorted(only_symbols(ups))
    down_symbols = sorted(only_symbols(downs))

    prev_up, prev_dn = load_last_signals()
    new_up = set(up_symbols) - prev_up
    new_dn = set(down_symbols) - prev_dn

    if not up_symbols and not down_symbols:
        for cid in recipients:
            send_message(cid, "✅ 今日无 MA60 趋势拐点（上涨/下跌）。")
            time.sleep(0.05)
    else:
        summary = (
            "🎊 今日 MA60 趋势拐点\n"
            f"📈 由跌转涨: {len(up_symbols)} 支 ✨新增 {len(new_up)} \n"
            f"📉 由涨转跌: {len(down_symbols)} 支 ✨新增 {len(new_dn)} \n"
            "------------"
        )
        for cid in recipients:
            send_message(cid, summary)
            time.sleep(0.05)
            if up_symbols:
                chunk_and_send_list(cid, "↗️ 上涨拐点：", up_symbols, highlight=new_up)
            if down_symbols:
                chunk_and_send_list(cid, "↘️ 下跌拐点：", down_symbols, highlight=new_dn)

    # 4) 如有异常标的，简要汇报（不阻断主流程）
    if bad:
        note = "以下标的数据异常 \n" + ", ".join(bad[:50])
        for cid in recipients:
            send_message(cid, note)
            time.sleep(0.05)

    # 5) 保存“今天的集合”，供明天对比
    save_last_signals(set(up_symbols), set(down_symbols))


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
