import yfinance as yf
import pandas as pd
import requests
import psycopg2
import os
import time
import threading
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# --- 보안 문 열어주기 (웹앱이 접속할 수 있게 허용) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. 데이터 수집 함수들 ---
def get_price(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    hist = ticker.history(period="1d")
    return round(float(hist['Close'].iloc[-1]), 2) if not hist.empty else 0

def get_rsi(ticker_symbol="QQQ", period=14):
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.history(period="1mo")
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, float('nan'))
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2)

def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        return round(float(data['fear_and_greed']['score']), 2)
    except:
        return 50

# --- 2. DB 저장 로직 ---
def update_database():
    print("🔄 데이터 수집을 시작합니다...")
    data = {
        'qqqm': get_price("QQQM"), 'qld': get_price("QLD"),
        'sgov': get_price("SGOV"), 'iau': get_price("IAU"),
        'vix': get_price("^VIX"), 'fx': get_price("KRW=X"),
        'rsi': get_rsi("QQQ"), 'fg': get_fear_and_greed()
    }
    db_url = os.environ.get("DB_URL") 
    if db_url:
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    id SERIAL PRIMARY KEY, qqqm_price FLOAT, qld_price FLOAT,
                    sgov_price FLOAT, iau_price FLOAT, vix FLOAT, fx FLOAT,
                    rsi FLOAT, fg FLOAT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                INSERT INTO market_data (id, qqqm_price, qld_price, sgov_price, iau_price, vix, fx, rsi, fg)
                VALUES (1, %(qqqm)s, %(qld)s, %(sgov)s, %(iau)s, %(vix)s, %(fx)s, %(rsi)s, %(fg)s)
                ON CONFLICT (id) DO UPDATE SET 
                    qqqm_price = EXCLUDED.qqqm_price, qld_price = EXCLUDED.qld_price,
                    sgov_price = EXCLUDED.sgov_price, iau_price = EXCLUDED.iau_price,
                    vix = EXCLUDED.vix, fx = EXCLUDED.fx, rsi = EXCLUDED.rsi, fg = EXCLUDED.fg,
                    updated_at = CURRENT_TIMESTAMP;
            """, data)
            conn.commit()
            cur.close()
            conn.close()
            print("💾 DB 저장 완료!")
        except Exception as e:
            print("❌ DB 저장 실패:", e)

# --- 3. 무한 반복 로봇 세팅 (핵심 수정 부분!) ---
def background_task():
    time.sleep(5) # 서버가 정문을 열 수 있도록 5초 양보합니다.
    while True:
        update_database()
        time.sleep(3600)

@app.on_event("startup")
def startup_event():
    # 식당 정문 업무와 별개로, 주방 뒤에서 로봇을 조용히 실행시킵니다.
    t = threading.Thread(target=background_task, daemon=True)
    t.start()

# --- 4. 웹 API 창구 ---
@app.get("/")
def read_root():
    return {"status": "Q7S3 Bot is Running OK!"}

@app.get("/api/data")
def get_latest_data():
    db_url = os.environ.get("DB_URL")
    if not db_url:
        return {"error": "DB_URL is missing"}
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT qqqm_price, qld_price, sgov_price, iau_price, vix, fx, rsi, fg, updated_at FROM market_data WHERE id=1;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if row:
            return {
                "qqqm": row[0], "qld": row[1], "sgov": row[2], "iau": row[3],
                "vix": row[4], "fx": row[5], "rsi": row[6], "fg": row[7],
                "updated_at": row[8]
            }
        else:
            return {"error": "No data yet. Waiting for first update..."}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
