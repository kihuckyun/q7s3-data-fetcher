import yfinance as yf
import requests
import psycopg2
import os
import asyncio  # 🌟 추가됨: 똑똑하게 쉬기 위한 도구
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# --- 보안 문 열어주기 ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 1. 데이터 수집 함수들 (수정 없음, 그대로 유지) ---
def get_price(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period="1d", interval="1m")
        if not hist.empty:
            return round(float(hist['Close'].iloc[-1]), 2)
        hist_d = ticker.history(period="1d")
        return round(float(hist_d['Close'].iloc[-1]), 2) if not hist_d.empty else 0
    except Exception as e:
        print(f"{ticker_symbol} 가격 수집 오류: {e}")
        return 0

def get_rsi(ticker_symbol="QQQ", period=14):
    try:
        ticker = yf.Ticker(ticker_symbol)
        df = ticker.history(period="1y")
        if df.empty: return 50
        delta = df['Close'].diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=period - 1, adjust=False).mean()
        ema_down = down.ewm(com=period - 1, adjust=False).mean()
        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs))
        return round(float(rsi.iloc[-1]), 2)
    except Exception as e:
        print(f"RSI 수집 오류: {e}")
        return 50

def get_fear_and_greed():
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
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
        return 50

# --- 2. DB 저장 로직 (🌟 with 구문으로 자동문 설치) ---
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
    if not db_url:
        return

    try:
        # 🌟 with 를 쓰면 에러가 나도 무조건 DB 문을 안전하게 닫아줍니다!
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS market_data (
                        id SERIAL PRIMARY KEY,
                        qqqm_price FLOAT, qld_price FLOAT, sgov_price FLOAT, iau_price FLOAT,
                        vix FLOAT, fx FLOAT, rsi FLOAT, fg FLOAT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                # with 구문이 끝나면 자동으로 commit() 과 close() 가 진행됩니다.
        print("💾 DB 저장 완료!")
    except Exception as e:
        print("❌ DB 저장 실패:", e)

# --- 3. 무한 반복 로봇 세팅 (🌟 비동기 방식으로 똑똑하게 쉬기) ---
async def background_task():
    await asyncio.sleep(5) # 서버가 열리길 5초 기다림
    while True:
        # 요리사(FastAPI)가 데이터 수집을 지시해두고 멈추지 않게 만듭니다.
        await asyncio.to_thread(update_database)
        await asyncio.sleep(3600) # 1시간 대기 (효율적인 휴식)

@app.on_event("startup")
async def startup_event():
    # 서버가 켜질 때 백그라운드 업무를 등록합니다.
    asyncio.create_task(background_task())

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
        # 🌟 여기도 with 구문 적용!
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT qqqm_price, qld_price, sgov_price, iau_price, vix, fx, rsi, fg, updated_at FROM market_data WHERE id=1;")
                row = cur.fetchone()
        
        if row:
            return {
                "qqqm": row[0], "qld": row[1], "sgov": row[2], "iau": row[3],
                "vix": row[4], "fx": row[5], "rsi": row[6], "fg": row[7], "updated_at": row[8]
            }
        else:
            return {"error": "No data yet. Waiting for first update..."}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
