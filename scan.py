import os, time, math, requests
import pandas as pd
from datetime import datetime, timedelta, timezone

FINNHUB_TOKEN = os.environ["FINNHUB_TOKEN"]
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
TICKERS = [t.strip().upper() for t in os.environ.get("TICKERS","AAPL,MSFT,GOOG").split(",") if t.strip()]

BASE = "https://finnhub.io/api/v1/stock/candle"

# 取最近 ~200 个日K，足够计算MA60
def fetch_daily_candles(symbol):
    # Finnhub stock/candle: symbol, resolution=D, from, to（UNIX秒）
    # 日线通常对拆分做调整，不对分红做现金调整；做MA60影响极小。 
    to_ts = int(datetime.now(timezone.utc).timestamp())
    frm_ts = int((datetime.now(timezone.utc) - timedelta(days=400)).timestamp())
    params = {
        "symbol": symbol,
        "resolution": "D",
        "from": frm_ts,
        "to": to_ts,
        "token": FINNHUB_TOKEN,
        "adjusted": "true",  # 一些SDK显示该参数，服务器会忽略/接受都无妨
    }
    for attempt in range(3):
        r = requests.get(BASE, params=params, timeout=20)
        if r.status_code == 429:  # 速率限制，等一等
            time.sleep(2.0)
            continue
        r.raise_for_status()
        data = r.json()
        if data.get("s") != "ok":
            return pd.DataFrame()
        df = pd.DataFrame({
            "t": pd.to_datetime(data["t"], unit="s", utc=True).tz_convert("UTC"),
            "close": data["c"],
        }).sort_values("t")
        return df
    return pd.DataFrame()

def detect_turnup(df, eps=0.0005):
    if df.empty:
        return None
    df = df.copy()
    df["sma60"] = df["close"].rolling(60, min_periods=60).mean()
    df["slope"] = df["sma60"].diff()
    if df["sma60"].isna().sum() > 0 or len(df) < 61:
        return None
    row_t = df.iloc[-1]
    row_t1 = df.iloc[-2]
    # 判定：昨天非正、今天为正，且相对变化超过阈值，减少“微抖动”
    if (row_t1["slope"] <= 0) and (row_t["slope"] > 0) and (row_t["slope"]/row_t["sma60"] > eps):
        return {
            "date": row_t.name.date() if hasattr(row_t.name, "date") else None,
            "close": float(row_t["close"]),
            "sma60": float(row_t["sma60"]),
            "slope": float(row_t["slope"]),
        }
    return None

def send_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}
    requests.post(url, json=payload, timeout=20)

def main():
    signals = []
    for i, sym in enumerate(TICKERS, 1):
        df = fetch_daily_candles(sym)
        sig = detect_turnup(df)
        if sig:
            signals.append((sym, sig))
        # 节流：~1.1秒/次 ≈ 54次/分钟，低于 Finnhub 免费限额 60/分
        time.sleep(1.1)

    if not signals:
        send_message("✅ 今日无 MA60 由降转升的标的。")
        return

    # 分多条发送，避免超长
    for sym, s in signals:
        text = (
            f"📈 {sym} MA60 由降转升\n"
            f"日期: {s['date']}\n"
            f"收盘: {s['close']:.2f}  SMA60: {s['sma60']:.2f}  Δ:{s['slope']:.4f}\n"
            f"图表: https://www.tradingview.com/symbols/{sym}/"
        )
        send_message(text)
        time.sleep(0.05)

if __name__ == "__main__":
    main()
