import yfinance as yf
import pandas as pd
import requests
import psycopg2
import os
import time

# --- 1. 데이터 수집 함수들 ---

def get_price(ticker_symbol):
    """현재가 가져오기"""
    ticker = yf.Ticker(ticker_symbol)
    hist = ticker.history(period="1d")
    return round(float(hist['Close'].iloc[-1]), 2) if not hist.empty else 0

def get_rsi(ticker_symbol="QQQ", period=14):
    """RSI(상대강도지수) 직접 계산하기"""
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.history(period="1mo") # 최근 한 달치 데이터
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, float('nan'))
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2)

def get_fear_and_greed():
    """CNN 피어 앤 그리드 지수 가져오기 (비공식 API 우회)"""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        score = data['fear_and_greed']['score']
        return round(float(score), 2)
    except:
        return 50 # 오류 시 중립값 50 반환

# --- 2. 메인 실행 로직 ---

def update_database():
    print("🔄 데이터 수집을 시작합니다...")
    
    # 데이터 수집
    data = {
        'qqqm': get_price("QQQM"),
        'qld': get_price("QLD"),
        'sgov': get_price("SGOV"),
        'iau': get_price("IAU"),
        'vix': get_price("^VIX"),       # VIX 지수
        'fx': get_price("KRW=X"),       # 원/달러 환율
        'rsi': get_rsi("QQQ"),          # QQQ RSI
        'fg': get_fear_and_greed()      # 피어앤그리드
    }
    
    print("✅ 수집 완료:", data)
    
    # --- 3. DB 저장 로직 (클라우드타입 PostgreSQL 연결) ---
    # 환경변수(DB_URL)에서 접속 정보를 가져옵니다.
    db_url = os.environ.get("DB_URL") 
    
    if db_url:
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            
            # 테이블이 없으면 만들기 (최초 1회 실행용)
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
            
            # 새 데이터 덮어쓰기 (항상 1번 줄(id=1)에 최신화)
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
    else:
        print("⚠️ DB_URL이 설정되지 않아 수집만 하고 종료합니다.")

if __name__ == "__main__":
    # 나중에 클라우드타입에서 스케줄러를 쓰지 않을 경우를 대비해 1시간(3600초)마다 반복 실행하게 만듭니다.
    while True:
        update_database()
        time.sleep(3600) # 1시간 대기 후 다시 수집