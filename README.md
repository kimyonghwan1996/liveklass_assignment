# Event Logging Pipeline Project (Airflow Version)

This project demonstrates a small data pipeline that generates mock user events, stores them into a database, performs analytical queries, and visualizes the results. 

To improve maintainability and strictly separate each process, the pipeline is fully orchestrated using **Apache Airflow**.

## Prerequisites
- Docker Engine & Docker Compose

## Quick Start
To launch the entire pipeline, simply run:
```bash
docker compose up
```

### What happens when you run this command?
1. A **PostgreSQL database** container (`event-db`) spins up.
2. An **Apache Airflow** container (`event-airflow`) starts in `standalone` mode.
   - It will automatically install the necessary Python packages (`pandas`, `matplotlib`, `seaborn`, `psycopg2-binary`).
   - It will mount the `./dags` folder and initialize its scheduler.
3. Access Airflow by opening **http://localhost:8080** in your web browser. 
   - **Username**: `admin`
   - **Password**: `admin`
4. Find the `event_logging_pipeline` DAG, unpause it, and trigger it (▶️ button).

### Pipeline Steps (Managed by Airflow API)
The Airflow DAG strictly manages the execution order of these tasks:
1. `init_db`: Establish connection and create `user_events` table in PostgreSQL.
2. `generate_events`: Create 1000 randomized events.
3. `store_events`: Insert the events into the database.
4. `analyze_and_visualize_data`: Perform SQL aggregation queries and save charts. 

---

## Step 1: Event Generation
The events are randomly generated through an Airflow `@task`. The DAG dynamically passes the generated events via XCom to the next step.

### Event Schema Design
I chose 4 kinds of events to represent real-world application workflows: 
1. `page_view`: Includes which `page` was viewed.
2. `purchase`: Includes `item_id` and purchase `amount`.
3. `error`: Includes HTTP `error_code` and `message`.
4. `signup`: No additional context needed.

**Why this design?**
By making the payload polymorphic (storing extra properties in a `JSONB` column structure), we can easily support infinite event types without heavily migrating strict table schemas, while indexing the fundamental standard metrics (`user_id`, `event_type`, `created_at`).

## Step 2: Log Storage
I chose **PostgreSQL** as the storage mechanism. 

**Why PostgreSQL?**
Instead of just appending raw JSON files or using SQLite, PostgreSQL handles concurrency easily. Moreover, it explicitly supports `JSONB` column types, blending the advantages of NoSQL's flexible schema with traditional SQL's robust aggregation features—an approach commonly used in modern analytical engineering.

## Step 3: Data Aggregation Analysis
Two queries were developed for data analysis (implemented via `pandas.read_sql` inside an Airflow task):

1. **Total events per type**:
   ```sql
   SELECT event_type, COUNT(*) as event_count
   FROM user_events
   GROUP BY event_type
   ORDER BY event_count DESC;
   ```

2. **Error Rate Trend Over Time**:
   Groups the data by daily dates to observe the application's overall error rate stability metric.

## Step 4 & 5: Results Visualization via Docker
A dashboard containing the data visualization is generated dynamically at the end of the DAG. You will find it successfully saved in this project repository inside the `./output/dashboard.png` path, thanks to local volume mapping from Docker.
