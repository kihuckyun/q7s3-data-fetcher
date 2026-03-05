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

# --- 1. 데이터 수집 함수들 (초정밀 공식 데이터 버전) ---
def get_price(ticker_symbol):
    """현재가 가져오기 (1분 단위 실시간 데이터 우선)"""
    try:
        ticker = yf.Ticker(ticker_symbol)
        # interval='1m'을 사용하여 장중 가장 최근 실시간 가격을 가져옵니다.
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            return round(float(hist['Close'].iloc[-1]), 2)
        
        # 혹시 1분 데이터가 없으면 기본 일봉 사용
        hist_d = ticker.history(period="1d")
        return round(float(hist_d['Close'].iloc[-1]), 2) if not hist_d.empty else 0
    except Exception as e:
        print(f"{ticker_symbol} 가격 수집 오류: {e}")
        return 0

def get_rsi(ticker_symbol="QQQ", period=14):
    """트레이딩뷰 공식(Wilder's Smoothing)과 동일한 RSI 계산"""
    try:
        ticker = yf.Ticker(ticker_symbol)
        # 정확도를 위해 1달이 아닌 '1년(1y)' 치 데이터를 가져옵니다.
        df = ticker.history(period="1y")
        if df.empty: return 50

        delta = df['Close'].diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)

        # 트레이딩뷰와 동일한 지수이동평균(EMA) 계산법 적용
        ema_up = up.ewm(com=period - 1, adjust=False).mean()
        ema_down = down.ewm(com=period - 1, adjust=False).mean()

        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs))
        return round(float(rsi.iloc[-1]), 2)
    except Exception as e:
        print(f"RSI 수집 오류: {e}")
        return 50

def get_fear_and_greed():
    """CNN 공식 API (강력한 봇 차단 우회 적용)"""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    # CNN 서버가 사람의 브라우저 접속으로 착각하게 만드는 강력한 헤더
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Referer': 'https://edition.cnn.com/'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        score = data['fear_and_greed']['score']
        return round(float(score), 2)
    except Exception as e:
        print(f"피어앤그리드 수집 오류(차단됨): {e}")
        return 50 # 최종 실패 시에만 50 반환

# --- 2. DB 저장 로직 ---
def update_database():
    print("🔄 데이터 수집을 시작합니다...")
    data = {
        'qqqm': get_price("QQQM"),
        'qld': get_price("QLD"),
        'sgov': get_price("SGOV"),
        'iau': get_price("IAU"),
        'vix': get_price("^VIX"),
        'fx': get_price("KRW=X"),
        'rsi': get_rsi("QQQ"),
        'fg': get_fear_and_greed()
    }
    print("✅ 수집 완료:", data)
    
    db_url = os.environ.get("DB_URL") 
    if db_url:
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    id SERIAL PRIMARY KEY,
                    qqqm_price FLOAT,
                    qld_price FLOAT,
                    sgov_price FLOAT,
                    iau_price FLOAT,
                    vix FLOAT,
                    fx FLOAT,
                    rsi FLOAT,
                    fg FLOAT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                INSERT INTO market_data (id, qqqm_price, qld_price, sgov_price, iau_price, vix, fx, rsi, fg)
                VALUES (1, %(qqqm)s, %(qld)s, %(sgov)s, %(iau)s, %(vix)s, %(fx)s, %(rsi)s, %(fg)s)
                ON CONFLICT (id) DO UPDATE SET 
                    qqqm_price = EXCLUDED.qqqm_price,
                    qld_price = EXCLUDED.qld_price,
                    sgov_price = EXCLUDED.sgov_price,
                    iau_price = EXCLUDED.iau_price,
                    vix = EXCLUDED.vix,
                    fx = EXCLUDED.fx,
                    rsi = EXCLUDED.rsi,
                    fg = EXCLUDED.fg,
                    updated_at = CURRENT_TIMESTAMP;
            """, data)
            conn.commit()
            cur.close()
            conn.close()
            print("💾 DB 저장 완료!")
        except Exception as e:
            print("❌ DB 저장 실패:", e)

# --- 3. 무한 반복 로봇 세팅 ---
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
                "qqqm": row[0],
                "qld": row[1],
                "sgov": row[2],
                "iau": row[3],
                "vix": row[4],
                "fx": row[5],
                "rsi": row[6],
                "fg": row[7],
                "updated_at": row[8]
            }
        else:
            return {"error": "No data yet. Waiting for first update..."}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
