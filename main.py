import os
import time
import random
import json
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_NAME = os.environ.get("DB_NAME", "event_logs")

NUM_EVENTS_PER_BATCH = 1000
EVENT_TYPES = ["page_view", "purchase", "error", "signup"]
RAW_DIR = "/app/data/raw"
OUTPUT_DIR = "/app/output"

# 메모리 상의 시뮬레이션 배포 상태
USER_COUNT = 100
users_state = {i: "Free" for i in range(1, USER_COUNT + 1)}

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
    )

def init_db():
    max_retries = 30
    conn = None
    for i in range(max_retries):
        try:
            conn = get_db_connection()
            break
        except psycopg2.OperationalError:
            print(f"데이터베이스 연결 대기 중... ({i + 1}/{max_retries})")
            time.sleep(2)
    
    if not conn:
        raise Exception("데이터베이스 연결 실패")

    with conn.cursor() as cur:
        # Staging 준비 테이블 생성
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stg_users_raw (
                raw_data JSONB,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS stg_events_raw (
                raw_data JSONB,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Marts (Dim/Fact) 변환 테이블 생성
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_user (
                user_sk SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                plan VARCHAR(50) NOT NULL,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP NOT NULL,
                is_current BOOLEAN NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fact_events (
                event_id SERIAL PRIMARY KEY,
                user_sk INT REFERENCES dim_user(user_sk),
                event_type VARCHAR(50) NOT NULL,
                event_data JSONB,
                created_at TIMESTAMP NOT NULL
            );
        """)
        
        # 실제 운영 환경에서는 매번 TRUNCATE 하지 않지만, 개발 주기 안정성을 위하여 리셋합니다.
        cur.execute("TRUNCATE TABLE dim_user CASCADE;")
        cur.execute("TRUNCATE TABLE stg_users_raw CASCADE;")
        cur.execute("TRUNCATE TABLE stg_events_raw CASCADE;")
    conn.commit()
    conn.close()
    print("ELT용 데이터베이스 상위 테이블 초기화 완료.")

def generate_and_dump_data(batch_timestamp):
    # 일부 사용자들의 랜덤 멤버십 등급 상승 시뮬레이션
    for i in range(1, USER_COUNT + 1):
        if users_state[i] == "Free" and random.random() < 0.05:
            users_state[i] = "Premium"
            
    # 사용자 정보 JSONL 파일 생성 (Extract)
    users_filepath = os.path.join(RAW_DIR, f"users_{batch_timestamp.strftime('%Y%m%d%H%M%S')}.jsonl")
    with open(users_filepath, 'w') as f:
        for u_id, plan in users_state.items():
            f.write(json.dumps({
                "user_id": u_id,
                "plan": plan,
                "updated_at": batch_timestamp.isoformat()
            }) + "\n")

    # 발생된 이벤트 정보 JSONL 덤프 (Extract)
    events_filepath = os.path.join(RAW_DIR, f"events_{batch_timestamp.strftime('%Y%m%d%H%M%S')}.jsonl")
    with open(events_filepath, 'w') as f:
        for _ in range(NUM_EVENTS_PER_BATCH):
            u_id = random.randint(1, USER_COUNT)
            event_type = random.choice(EVENT_TYPES)
            event_data = {}
            if event_type == "purchase":
                # 프리미엄 회원의 결제 시 가중치 부여 (더 많이 산다!)
                multiplier = 2.0 if users_state[u_id] == "Premium" else 1.0
                event_data["amount"] = round(random.uniform(10.0, 100.0) * multiplier, 2)
            elif event_type == "page_view":
                event_data["page"] = random.choice(["/home", "/product", "/checkout", "/premium_only_feature"])
            elif event_type == "error":
                event_data["error_code"] = random.choice([404, 500])
                
            f.write(json.dumps({
                "user_id": u_id,
                "event_type": event_type,
                "event_data": event_data,
                "created_at": batch_timestamp.isoformat() # 순서 꼬임을 방지하기 위해 배치 시간을 직접 할당
            }) + "\n")
            
    return users_filepath, events_filepath

def execute_elt(users_filepath, events_filepath, batch_timestamp):
    conn = get_db_connection()
    with conn.cursor() as cur:
        # --- 1. STAGING 직접 로드 (LOAD) ---
        # 새 배치 처리를 위해 임시 적재 테이블을 비웁니다.
        cur.execute("TRUNCATE TABLE stg_users_raw;")
        cur.execute("TRUNCATE TABLE stg_events_raw;")
        
        # 원본 JSONL 파일에 쌓인 데이터를 한 줄씩 준비 테이블(Staging)에 등록
        with open(users_filepath, 'r') as f:
            for line in f:
                cur.execute("INSERT INTO stg_users_raw (raw_data) VALUES (%s)", (line.strip(),))
        with open(events_filepath, 'r') as f:
            for line in f:
                cur.execute("INSERT INTO stg_events_raw (raw_data) VALUES (%s)", (line.strip(),))

        # --- 2. 차원(DIMENSION) 데이터 가공 (TRANSFORM) ---
        # "사용자 가입 이력 유지(SCD Type 2)" 
        merge_sql = """
        WITH staging AS (
            SELECT 
                (raw_data->>'user_id')::INT AS user_id,
                raw_data->>'plan' AS plan,
                (raw_data->>'updated_at')::TIMESTAMP AS updated_at
            FROM stg_users_raw
        ),
        current_dim AS (
            SELECT user_sk, user_id, plan, valid_from
            FROM dim_user
            WHERE is_current = TRUE
        ),
        updates AS (
            SELECT s.user_id, s.plan, s.updated_at, c.user_sk AS old_sk, c.plan AS old_plan
            FROM staging s
            LEFT JOIN current_dim c ON s.user_id = c.user_id
        )
        -- 상태가 뒤바뀐 이전 회원 기록을 만료(Terminate) 시킴
        UPDATE dim_user SET valid_to = u.updated_at, is_current = FALSE
        FROM updates u
        WHERE dim_user.user_sk = u.old_sk AND u.old_sk IS NOT NULL AND u.plan != u.old_plan;
        """
        cur.execute(merge_sql)
        
        insert_sql = """
        WITH staging AS (
            SELECT 
                (raw_data->>'user_id')::INT AS user_id,
                raw_data->>'plan' AS plan,
                (raw_data->>'updated_at')::TIMESTAMP AS updated_at
            FROM stg_users_raw
        ),
        current_dim AS (
            SELECT user_id, plan
            FROM dim_user
            WHERE is_current = TRUE
        )
        INSERT INTO dim_user (user_id, plan, valid_from, valid_to, is_current)
        SELECT 
            s.user_id, 
            s.plan, 
            s.updated_at, 
            '9999-12-31'::TIMESTAMP, 
            TRUE
        FROM staging s
        LEFT JOIN current_dim c ON s.user_id = c.user_id
        WHERE c.user_id IS NULL OR c.plan != s.plan;
        """
        cur.execute(insert_sql)

        # --- 3. 팩트 데이터(FACTS) 삽입 가공 (TRANSFORM) ---
        # 이벤트 발송 시간을 이용해 가장 합당한 시기의 회원 과거 이력 매핑
        fact_sql = """
        INSERT INTO fact_events (user_sk, event_type, event_data, created_at)
        SELECT 
            d.user_sk,
            s.raw_data->>'event_type',
            (s.raw_data->>'event_data')::JSONB,
            (s.raw_data->>'created_at')::TIMESTAMP
        FROM stg_events_raw s
        JOIN dim_user d ON (s.raw_data->>'user_id')::INT = d.user_id
        WHERE (s.raw_data->>'created_at')::TIMESTAMP >= d.valid_from 
          AND (s.raw_data->>'created_at')::TIMESTAMP < d.valid_to;
        """
        cur.execute(fact_sql)

    conn.commit()
    conn.close()
    print(f"[{batch_timestamp}] 배치 ELT 처리 성공 완료.")

def analyze_and_visualize_data():
    conn = get_db_connection()
    
    # 1. 회원 등급 이력별 전체 결제 횟수 측정 및 수익 추적
    query1 = """
        SELECT 
            d.plan,
            COUNT(f.event_id) as event_count,
            COALESCE(SUM((f.event_data->>'amount')::NUMERIC), 0) as total_revenue
        FROM fact_events f
        JOIN dim_user d ON f.user_sk = d.user_sk
        WHERE f.event_type = 'purchase'
        GROUP BY d.plan;
    """
    df_revenue = pd.read_sql(query1, conn)
    print("\n--- 회원 등급별 발생 파생 수익 ---")
    print(df_revenue)

    # 2. 회원 상태별 이벤트 행동 분포도 파악
    query2 = """
        SELECT 
            d.plan,
            f.event_type,
            COUNT(f.event_id) as event_count
        FROM fact_events f
        JOIN dim_user d ON f.user_sk = d.user_sk
        GROUP BY d.plan, f.event_type
        ORDER BY d.plan, f.event_type;
    """
    df_distribution = pd.read_sql(query2, conn)
    
    conn.close()
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    sns.set_theme(style="whitegrid")
    
    if not df_revenue.empty:
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
        
        sns.barplot(data=df_revenue, x='plan', y='total_revenue', ax=axes[0], palette="magma")
        axes[0].set_title("Total Revenue by User Plan", fontsize=14)
        axes[0].set_xlabel("Membership Plan")
        axes[0].set_ylabel("Revenue ($)")
        
        sns.barplot(data=df_distribution, x='event_type', y='event_count', hue='plan', ax=axes[1], palette="Set2")
        axes[1].set_title("Event Distribution by User Plan", fontsize=14)
        axes[1].set_xlabel("Event Action Type")
        axes[1].set_ylabel("Event Count")
        
        plt.tight_layout()
        output_path = os.path.join(OUTPUT_DIR, "dashboard.png")
        plt.savefig(output_path)
        print(f"시각화 결과 이미지 저장 완료: {output_path}")
        plt.close()

if __name__ == "__main__":
    print("ETL / ELT 데이터 웨어하우스 파이프라인 가동 시작...")
    # 원시 덤프 데이터용 내부 디렉토리 안정성 보장
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR)

    init_db()
    
    # 초반에 과거 5일 치 시뮬레이션 데이터를 단숨에 로드해버립니다
    # 이후 10분 대기 모드로 진입합니다
    base_time = datetime.now() - timedelta(days=5)
    
    print("과거 5일 치 시뮬레이션 이벤트 데이터 생성 중...")
    for day_offset in range(5):
        batch_time = base_time + timedelta(days=day_offset)
        u_file, e_file = generate_and_dump_data(batch_time)
        execute_elt(u_file, e_file, batch_time)
    
    analyze_and_visualize_data()
    print("과거 데이터 적재 및 변환 처리 완료.")
    
    # 실시간 데이터 스트리밍 연속 루프
    while True:
        try:
            print("현재 배치 처리 완료. 다음 배치 파이프라인 트리거 전까지 10분간 대기합니다...")
            time.sleep(600)
            
            now = datetime.now()
            u_file, e_file = generate_and_dump_data(now)
            execute_elt(u_file, e_file, now)
            analyze_and_visualize_data()
        except Exception as e:
            print(f"파이프라인 실행 중 심각한 에러 발생: {e}")
            time.sleep(60)
