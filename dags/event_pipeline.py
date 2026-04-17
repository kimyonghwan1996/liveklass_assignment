import os
import time
import random
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Database configuration (pulled from environment inside standard Airflow setup)
DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_NAME = os.environ.get("DB_NAME", "event_logs")

# Constants
NUM_EVENTS = 1000
EVENT_TYPES = ["page_view", "purchase", "error", "signup"]
OUTPUT_DIR = "/opt/airflow/output"

def get_db_connection():
    """Helper to get a DB connection"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    return conn

@dag(
    dag_id="event_logging_pipeline",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["assignment"]
)
def event_pipeline_dag():

    @task()
    def init_db():
        """Create the events table if it does not exist"""
        import psycopg2
        
        # Wait up to a minute for DB to be available
        max_retries = 30
        conn = None
        for i in range(max_retries):
            try:
                conn = get_db_connection()
                print("Successfully connected to the database!")
                break
            except psycopg2.OperationalError:
                print(f"Waiting for database to be ready... ({i + 1}/{max_retries})")
                time.sleep(2)
        
        if not conn:
            raise Exception("Database connection failed")

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_events (
                    id SERIAL PRIMARY KEY,
                    user_id INT NOT NULL,
                    event_type VARCHAR(50) NOT NULL,
                    event_data JSONB,
                    created_at TIMESTAMP NOT NULL
                );
            """)
            # Clear existing data for fresh run
            cur.execute("TRUNCATE TABLE user_events;")
        conn.commit()
        conn.close()
        print("Database initialized.")

    @task(multiple_outputs=False)
    def generate_events():
        """Generate mock user events and pass them down"""
        events = []
        now = datetime.now()
        
        for _ in range(NUM_EVENTS):
            user_id = random.randint(1, 100)
            event_type = random.choice(EVENT_TYPES)
            
            days_ago = random.uniform(0, 7)
            created_at = now - timedelta(days=days_ago) # Store actual python datetime object
            
            event_data = {}
            if event_type == "purchase":
                event_data["amount"] = round(random.uniform(10.0, 500.0), 2)
                event_data["item_id"] = random.randint(1000, 9999)
            elif event_type == "page_view":
                event_data["page"] = random.choice(["/home", "/product", "/checkout", "/about"])
            elif event_type == "error":
                event_data["error_code"] = random.choice([404, 500, 403, 502])
                event_data["message"] = "An error occurred"
                
            # Serialize the timestamp to string safely since XCom must be JSON serializable
            events.append((user_id, event_type, json.dumps(event_data), created_at.isoformat()))
            
        print(f"Generated {NUM_EVENTS} mock events.")
        return events

    @task()
    def store_events(events):
        """Store generated events into the database safely"""
        conn = get_db_connection()
        query = """
            INSERT INTO user_events (user_id, event_type, event_data, created_at)
            VALUES %s
        """
        with conn.cursor() as cur:
            execute_values(cur, query, events)
        conn.commit()
        conn.close()
        print("Events stored in the database.")

    @task()
    def analyze_and_visualize_data():
        """Run aggregation queries and save charts"""
        conn = get_db_connection()
        
        # Query 1: Event type counts
        query1 = """
            SELECT event_type, COUNT(*) as event_count
            FROM user_events
            GROUP BY event_type
            ORDER BY event_count DESC;
        """
        df_counts = pd.read_sql(query1, conn)
        print("\n--- Event Counts by Type ---")
        print(df_counts)

        # Query 2: Error rate over time (by day)
        query2 = """
            SELECT 
                DATE(created_at) as event_date,
                COUNT(*) as total_events,
                SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) as error_events
            FROM user_events
            GROUP BY DATE(created_at)
            ORDER BY event_date ASC;
        """
        df_trend = pd.read_sql(query2, conn)
        df_trend['error_rate'] = (df_trend['error_events'] / df_trend['total_events']) * 100
        print("\n--- Error Rate by Day ---")
        print(df_trend)
        
        conn.close()
        
        # Visualization
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            
        sns.set_theme(style="whitegrid")
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
        
        sns.barplot(data=df_counts, x='event_type', y='event_count', ax=axes[0], palette="viridis")
        axes[0].set_title("Total Events by Type", fontsize=14)
        axes[0].set_xlabel("Event Type")
        axes[0].set_ylabel("Count")
        
        sns.lineplot(data=df_trend, x='event_date', y='error_rate', ax=axes[1], marker='o', color='red')
        axes[1].set_title("Error Rate Trend (%) Over Time", fontsize=14)
        axes[1].set_xlabel("Date")
        axes[1].set_ylabel("Error Rate (%)")
        
        plt.tight_layout()
        output_path = os.path.join(OUTPUT_DIR, "dashboard.png")
        plt.savefig(output_path)
        print(f"Visualization saved to: {output_path}")

    # Define DAG dependencies
    events_data = generate_events()
    # init_db must run before we store, and generate can run completely in parallel, 
    # but for simplicity we chain dependencies.
    init_db() >> events_data
    store_events(events_data) >> analyze_and_visualize_data()

# Instantiate the DAG
event_pipeline = event_pipeline_dag()
