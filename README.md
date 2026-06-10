
# 🏢 NOVA RETAIL GROUP: Dockerized On-Premise Data Platform with Airflow-Orchestrated Medallion ETL
*A containerized, scheduled, three-layer Spark pipeline I built for an on-premise enterprise retail scenario, processing transactional data through PostgreSQL with Airflow orchestration and parameterized stock-vs-scaled execution.*

---

## 🏷️ Badges
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![PySpark](https://img.shields.io/badge/PySpark-4.1.1-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-containerized-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9.2-darkorange)
![Medallion](https://img.shields.io/badge/Architecture-Medallion-success)
![Source Rows](https://img.shields.io/badge/Source%20Rows-555K-red)
![Stock Runtime](https://img.shields.io/badge/Stock%20Runtime-3m%2011s-success)

---

# 🏢 The Client Scenario

**Nova Retail Group** is a fictional multinational retail and logistics organization specializing in e-commerce operations, product distribution, and customer fulfillment services. Their analysts and operations teams depend on a centralized PostgreSQL warehouse to answer questions about sales performance, regional behavior, and product profitability. Their existing pipeline was a collection of manually executed Python scripts against fragmented database environments, with no historical preservation and no orchestration.

The scenario brief identified four specific engineering constraints. **Fragmented infrastructure**: the organization lacked a standardized environment for reproducible local deployment. **Manual ETL execution**: workflows depended on operators remembering to run scripts in the right order. **Limited historical data preservation**: raw responses were transformed in flight and discarded, so re-running analysis with new research parameters meant returning to the source. **Runtime and performance bottlenecks**: in-memory processing tools struggled with growing transactional volumes.

I was engaged as an on-premise data engineer responsible for transitioning Nova Retail from this fragmented, manual environment to a containerized, scheduled, distributed data platform. The deliverable had to do five things: deploy PostgreSQL reproducibly via Docker, build distributed PySpark ETL pipelines, implement Bronze/Silver/Gold medallion architecture, automate execution through orchestration, and support both production scheduling and ad-hoc scaled testing.

> *This project was built against a structured case-study brief from an ACTD-accredited (American Council of Training and Development) data engineering scenario.*

> *Note on dataset: Nova Retail uses the public Olist Brazilian e-commerce dataset. This same dataset appears in [PayFlow](https://github.com/yoismail/payflow_case_study) earlier in this portfolio. PayFlow demonstrates classical normalized warehousing; Nova Retail demonstrates production-grade infrastructure (Docker, Airflow, medallion). Different engineering angles on the same dataset.*

---

# 🎯 The Business Problem

The brief identified four specific constraints blocking Nova Retail's analytics:

1. **Fragmented infrastructure management.** No standardized environment for reproducible deployment. Each operator's machine was its own configuration story.
2. **Manual ETL execution.** Pipelines required someone to run them, in the right order, by hand. No scheduling. No retry on failure. No visibility into history.
3. **Limited historical data preservation.** Without a Bronze/Silver/Gold separation, raw data wasn't archived. Re-processing required re-ingesting from source.
4. **Runtime bottlenecks at growing volumes.** Pandas-based in-memory pipelines struggled past ~100K-row datasets.

Left unaddressed, Nova Retail's operations and analytics teams would continue waiting on manual processes, working from partial data, and burning operator time on what should be infrastructure work. The brief required a pipeline that could be deployed reproducibly, scheduled reliably, and validated at scale.

---

# 🌟 What I Built

A containerized data platform with five interlocking pieces:

1. **Dockerized PostgreSQL warehouse** with persistent volumes and automatic DDL execution on first start.
2. **Containerized Spark execution environment** built from a custom Dockerfile, with the PostgreSQL JDBC driver pre-installed.
3. **Distributed PySpark medallion pipeline** (Bronze raw Parquet → Silver cleaned and joined → Gold three-table dimensional model) with year/month partitioning on fact-shaped tables, broadcast hints on small dimensions, and explicit cache strategy on the join chain.
4. **Apache Airflow orchestration** that schedules the pipeline daily and supports parameterized on-demand execution for either stock data (analytical truth) or scaled data (architecture stress testing).
5. **A three-table dimensional Gold layer** that lands directly in PostgreSQL with row-count validation on every load.

End-to-end, the stock pipeline processes ~555,000 source rows from 7 raw datasets into 118,431 cleaned Silver records and three analytical Gold tables, in 3 minutes 11 seconds on a single laptop. Architecture validation at 2x scale processes ~1.1 million source rows into 7.5 million Silver records and the same three Gold tables in 13 minutes 14 seconds, demonstrating the design handles substantially larger volumes than current production scale.

---

# 🧠 What I Demonstrate in This Project

### 🔹 Containerized Infrastructure with Docker Compose
The entire data platform runs as Docker containers orchestrated by `docker-compose.yml`. PostgreSQL 15 in one container, Spark in another (built from `spark.Dockerfile`), and Airflow's webserver + scheduler + init in three more. Persistent named volumes keep data across restarts. A health check on PostgreSQL gates dependent services from starting before the database is actually ready. Five services, one command (`docker-compose up`) to bring the whole stack online.

### 🔹 Custom Spark Image with JDBC Driver Pre-Installed
Spark runs from a custom image built on `apache/spark:4.1.1`. The Dockerfile installs Python dependencies (`pandas`, `psycopg2-binary`, `python-dotenv`, `numpy`) and adds the PostgreSQL JDBC driver (`postgresql-42.7.4.jar`) directly into Spark's `/opt/spark/jars/` at build time. Result: when the pipeline runs, the JDBC driver is already present. No runtime download. No version drift. The image is the contract.

### 🔹 Sidecar Spark Execution Pattern via Airflow
The Spark container runs `sleep infinity` and stays alive as a long-lived execution environment. Airflow doesn't run Spark itself. Instead, Airflow's `BashOperator` calls `docker exec spark /opt/spark/bin/spark-submit ...` to inject jobs into the running Spark container. This is sometimes called the "sidecar Spark" pattern. It keeps orchestration decoupled from execution, lets Airflow restart independently of Spark, and is the same shape used when deploying against a managed Spark cluster (Databricks, EMR, Dataproc) where you'd `docker exec` becomes a cluster-submit API call.

### 🔹 Parameterized DAG for Stock vs Scaled Execution
The Airflow DAG accepts two parameters via Airflow's native `Param` API: `scale_data` (boolean, defaults to false) and `scale_multiplier` (integer 1-5, defaults to 2). Scheduled runs (daily at 06:00 UTC) use the defaults, producing analytical truth from real data. Manual triggers via the Airflow UI surface a form letting the operator request a scaled run for architecture validation. **Same DAG, two purposes, controlled at trigger time.**

### 🔹 Three Idempotency Models, One Per Layer
Bronze writes raw Parquet with `mode="overwrite"`. Silver writes joined Parquet with `mode="overwrite"`. Gold loads to PostgreSQL with `mode="overwrite"`. The honest tradeoff: simple, deterministic, idempotent. Running the pipeline twice in a row produces the same warehouse state. Re-running after a failure picks up cleanly. The cost is that every run rebuilds every layer; for the data volumes Nova Retail operates at, that cost is negligible compared to the simplicity it buys.

### 🔹 Year/Month Partitioning on Fact-Shaped Bronze Tables
The `orders` and `order_items` Bronze writes use `.partitionBy("order_year", "order_month")`, deriving partition columns from `order_purchase_timestamp` and `shipping_limit_date` respectively. This isn't a transformation of the data; the source columns are preserved. The partition columns are storage-layout metadata that enable partition pruning on read. At current scale this is invisible. At any scale where data grows, this is the single biggest architectural lever: Silver and downstream consumers can read only the partitions they need instead of scanning all of Bronze. Small dimension tables (`customers`, `sellers`, `products`) write unpartitioned because they're too small to benefit and partitioning would just create directory clutter.

### 🔹 Explicit Spark Schemas Across All 7 Datasets
Every CSV read uses an explicit `StructType` schema declared at module top, not `inferSchema=True`. The cost is a one-time schema declaration. The benefit is two-fold: Spark skips the inference pass (halving CSV I/O cost) and types are guaranteed to be correct (the `review_score` column reads as integer instead of being silently inferred as string). For a project where the source data contract is documented and stable, explicit schemas are the production-grade choice.

### 🔹 Broadcast Hints on Small Dimension Joins
The Silver layer joins seven tables. Three of them (`sellers` at 3K rows, `products` at 33K, and at smaller scales `customers` at 99K) are small enough to broadcast to every Spark executor instead of shuffling. Explicit `broadcast()` hints in the join chain make this deterministic instead of relying on Spark's auto-broadcast threshold. The `customers` broadcast was removed during testing when 5x scaling caused JVM memory pressure (customers at 5x = ~500K rows, too large to safely broadcast). The remaining broadcasts on `products` and `sellers` are stable across all tested scales.

### 🔹 Explicit Cache Strategy on the Join Chain
The Silver layer caches its joined output via `silver_df.cache()` before any action. The Gold layer caches `fact_sales` after the column selection because it's used four times downstream (count + write + two summary aggregations). Without caching, Spark may recompute these chains on each subsequent action. With caching, the join work happens once and the result is held in memory (with disk spillover via `MEMORY_AND_DISK` semantics) for everything downstream.

### 🔹 Three-Table Dimensional Gold Layer
The Gold layer produces three analytical tables, not one. **`fact_sales`** is a 16-column transaction-grain fact table. **`sales_summary`** aggregates by (customer_state, product_category_name) for state-level category insights. **`sales_by_month_state`** aggregates by (year-month, customer_state) for time-series queries. Each has its own primary key in PostgreSQL, its own indexes optimized for the queries the analytics layer runs against it. This is a meaningful step up from a single-table mart: the warehouse serves three distinct query patterns natively.

### 🔹 Row-Count Validation on Every Load
After each PostgreSQL write, the loader reads back the target table via JDBC and counts rows. If the count matches the source DataFrame, validation passes. If not, a warning is logged. This is the same defensive pattern that catches silent data loss in production: writes that "succeed" at the protocol level but lose rows due to constraint violations, transaction rollbacks, or driver bugs. Three Gold tables, three validation passes per run.

### 🔹 Dual-Host Configuration Switching
The `db_config.py` module detects whether it's running inside a Docker container (via the presence of `/.dockerenv`) and switches between `localhost` and the Docker network hostname `retail_postgres` accordingly. Same Python runs locally during development and inside containers in production without code changes. Environment variables are loaded from `/opt/retail_project/.env` inside containers or from the current directory locally. Five required env vars are validated at import time; the module refuses to load if anything is missing.

### 🔹 Observability Through a Shared Logger Module
A custom `logger.py` provides the `@timed` decorator (logs duration in human-readable form, "3m 11s"), `section()` for visual banners between pipeline stages, cross-platform UTF-8 detection for emoji-safe console output, a rotating file handler (2MB rotation, 7 backups) for `pipeline.log`, and SQLAlchemy engine-noise suppression. The same module is used across this project, [XTD Research Labs](https://github.com/yoismail/xtd_research_labs_case_study), and [FibbieBanks](https://github.com/yoismail/fibbie_banks). Consistency across the portfolio is itself a signal.

---

# 🌐 High-Level Architecture Diagram

```
                ┌─────────────────────────────────────────┐
                │      Apache Airflow (containerized)     │
                │  • Webserver UI on :8080                │
                │  • Scheduler triggers DAG daily 06:00   │
                │  • Param form for stock vs scaled       │
                └─────────────────┬───────────────────────┘
                                  │ docker exec
                                  ▼
                ┌─────────────────────────────────────────┐
                │      Spark Container (custom image)     │
                │  • apache/spark:4.1.1 base              │
                │  • PostgreSQL JDBC driver pre-installed │
                │  • Sleeps until exec'd by Airflow       │
                └─────────────────┬───────────────────────┘
                                  ▼
        ┌─────────────────────────────────────────────────────┐
        │                  PySpark Pipeline                   │
        │                                                     │
        │  STEP 1: EXTRACT (explicit StructType schemas)      │
        │     7 CSVs → DataFrames                             │
        │     Optional 1-5x replication for scale tests       │
        │                                                     │
        │  STEP 2: BRONZE (year/month partitioning)           │
        │     7 raw Parquet outputs                           │
        │     orders + order_items partitioned by year/month  │
        │                                                     │
        │  STEP 3: SILVER (broadcast joins + cache)           │
        │     7 tables joined to 1 wide frame                 │
        │     broadcast() on sellers + products               │
        │     .cache() before downstream actions              │
        │                                                     │
        │  STEP 4: GOLD (3-table dimensional model)           │
        │     fact_sales (transaction grain)                  │
        │     sales_summary (state × category)                │
        │     sales_by_month_state (year-month × state)       │
        │                                                     │
        │  STEP 5: LOAD (JDBC + row-count validation)         │
        │     3 writes to retail_gold schema                  │
        │     Read-back validation on each                    │
        └─────────────────┬───────────────────────────────────┘
                          ▼
        ┌─────────────────────────────────────────────────────┐
        │      PostgreSQL Container (postgres:15-alpine)      │
        │  • Persistent named volume                          │
        │  • DDL auto-runs from /docker-entrypoint-initdb.d   │
        │  • Health check gates dependent services            │
        │  • retail_gold schema with 3 tables + indexes       │
        └─────────────────┬───────────────────────────────────┘
                          ▼
        ┌─────────────────────────────────────────────────────┐
        │     BI / Analyst Consumers (Tableau, Power BI,      │
        │     Jupyter, direct psql)                           │
        │     8 production queries in sql/queries.sql         │
        └─────────────────────────────────────────────────────┘
```

---

# 🗂 Project Structure

```
NOVA_RETAIL_CASE_STUDY/
├── airflow/
│   ├── dags/
│   │   └── retail_etl_pipeline.py    # DAG with Param (stock/scaled)
│   ├── logs/                          # Airflow's own logs (gitignored)
│   └── plugins/                       # Empty plugin folder
│
├── data/
│   ├── raw/                           # 7 Olist source CSVs (gitignored)
│   ├── bronze/                        # Partitioned Parquet (gitignored)
│   ├── silver/                        # Cleaned/joined Parquet (gitignored)
│   └── gold/                          # 3 analytical CSVs (gitignored)
│
├── etl/
│   ├── spark_session.py               # SparkSession factory, tuned config
│   ├── extract.py                     # CSVs → DataFrames, explicit schemas
│   ├── scale_data.py                  # Row-replication scaling utility
│   ├── bronze.py                      # Raw Parquet writes, partitioned facts
│   ├── silver.py                      # Joins, cleaning, broadcast hints, cache
│   ├── gold.py                        # 3-table dimensional model
│   ├── load.py                        # JDBC writes + row-count validation
│   ├── db_config.py                   # Env-var loader, dual-host switching
│   ├── logger.py                      # Cross-platform logging utilities
│   └── run_pipeline.py                # Orchestrator, single Spark session
│
├── logs/
│   └── pipeline.log                   # Rotating UTF-8 file log
│
├── sql/
│   ├── create_tables.sql              # DDL: 3 tables, composite PKs, indexes
│   └── queries.sql                    # 8 production analytical queries
│
├── .env                               # DB + Airflow credentials (gitignored)
├── .gitignore
├── docker-compose.yml                 # 5-service container stack
├── spark.Dockerfile                   # Custom Spark image w/ JDBC
├── README.md
└── requirements.txt                   # Pinned PySpark, others unpinned
```

---

# 🔄 Pipeline Flow

The pipeline runs as five explicit stages, all coordinated by `run_pipeline.py` through a single shared Spark session. There is no chained-`main()` pattern (each stage's `main()` is for standalone debugging only); production execution flows entirely through the orchestrator. The Spark session is created once at the start, threaded through every stage, and shut down in a `finally` block regardless of success or failure.

### 1️⃣ Extract — Explicit Schemas, Optional Scaling
`extract()` reads 7 Olist CSVs into Spark DataFrames using explicit `StructType` schemas declared at module top. If `SCALE_DATA=true`, each DataFrame is replicated by `SCALE_MULTIPLIER` (default 2) via the `scale_data` utility. Failures across all 7 CSVs are collected and raised as a single summary exception at the end, so the pipeline fails loud with full visibility into which files broke and why.

### 2️⃣ Bronze — Partitioned Parquet for Fact Tables
`bronze_layer()` writes each DataFrame to `data/bronze/{name}/` as Parquet. The `orders` and `order_items` tables get partitioned by year/month derived from their respective timestamp columns (`order_purchase_timestamp` and `shipping_limit_date`). Small dimension tables (`customers`, `sellers`, `products`) write unpartitioned. All writes use `mode="overwrite"` for clean idempotency.

### 3️⃣ Silver — Joined, Cleaned, Cached
`silver_layer()` joins all seven tables with LEFT joins anchored on `orders`. Small dimensions (`products`, `sellers`) get explicit `broadcast()` hints to skip shuffle joins. Numeric columns are cast to `DECIMAL(10,2)` for aggregation precision. Date parts (year, month, day, weekday, year-month) are extracted from `order_purchase_timestamp`. Rows with NULL `payment_value` are filtered out. The result is cached before the row count, write, and downstream consumption by the Gold layer.

### 4️⃣ Gold — Three-Table Dimensional Model
`gold_layer()` consumes the cached Silver DataFrame and produces three analytical tables:

- **`fact_sales`** (16 columns, transaction grain): order details + denormalized state/category/payment attributes for lightweight querying
- **`sales_summary`** (5 columns): aggregated by (customer_state, product_category_name) for state-level category analysis
- **`sales_by_month_state`** (5 columns): aggregated by (order_year_month, customer_state) for time-series queries

`fact_sales` is itself cached because it's used four times (count + write + two aggregations). All three tables write to `data/gold/{name}/` as Parquet and return as a 3-tuple to the orchestrator.

### 5️⃣ Load — JDBC Writes with Row-Count Validation
`load_df_to_postgres()` writes each Gold DataFrame to PostgreSQL via Spark's native JDBC connector. Each write uses `batchsize=10000` for chunked inserts. After each write, the function reads back the target table via JDBC, counts rows, and compares against the source DataFrame's count. If counts match, validation passes. If not, a warning is logged. Three Gold tables, three writes, three validation checks per pipeline run.

---

# 📊 Real Performance Numbers (Captured June 2026)

These come from actual `pipeline.log` files, not estimates.

### Stock Run (Production Baseline)

| Stage | Duration |
|---|---|
| Spark session creation | ~36s |
| Extract (7 CSVs) | ~19s |
| Bronze (7 datasets, 2 partitioned) | ~31s |
| Silver (7-way join, cache, write) | ~28s |
| Gold (3 tables built from cached fact_sales) | ~39s |
| Load (3 tables to PostgreSQL + validation) | ~39s |
| **End-to-end** | **3m 11s** |

### Architecture Validation Run (2x Scaled)

| Stage | Duration |
|---|---|
| Spark session creation | ~59s |
| Extract (7 CSVs scaled 2x) | ~39s |
| Bronze (7 datasets at 2x volume) | ~59s |
| Silver (joined to 7.5M records) | ~2m 8s |
| Gold (3 tables from 7.5M-row cached fact_sales) | ~59s |
| Load — fact_sales (7.5M rows + validation) | ~7m 56s |
| Load — other two Gold tables | ~30s |
| **End-to-end** | **13m 14s** |

### Volume Throughput

| Layer | Stock Records | 2x Scaled Records | Notes |
|---|---|---|---|
| Source CSVs (combined) | ~555,626 | ~1.1M | 7 raw Olist files |
| Bronze (combined) | ~555,626 | ~1.1M | Partitioned where appropriate |
| Silver | 118,431 | 7,533,104 | 2x scaling produces join multiplication (see Scaling Methodology below) |
| Gold — fact_sales | 118,431 | 7,533,104 | Transaction grain |
| Gold — sales_summary | 1,394 | 1,394 | (customer_state, category) groups |
| Gold — sales_by_month_state | 565 | 565 | (year-month, state) groups |

### Load Validation

Every Postgres load passed row-count validation on both runs. Zero data loss across the warehouse.

---

# 🔬 Scaling Methodology (Honest Framing)

The scaling capability is **purely architectural validation, not analytical**. The stock run produces analytical truth from real Olist data. The scaled run proves the pipeline holds up under volumes substantially larger than current production scale.

### Why Silver Multiplies Beyond the Scale Factor

`scale_data.py` replicates each source DataFrame N times via `union`. This means at 2x scaling, every row appears twice — including duplicated keys. When the Silver layer joins these tables on shared keys, the duplicated keys multiply across joins. With 6 tables contributing duplicated keys to the join chain, the row count multiplies roughly as 2^6 = 64x.

This is **expected behavior of row-replication testing**. The 7,533,104 Silver figure represents pipeline throughput, not analytically distinct sales. A pipeline that successfully processes 7.5M Silver records, writes them to PostgreSQL, and validates row counts has proven its row-handling capacity. It has not proven analytical correctness at scale; for that, key-replication-with-offset or genuinely synthetic data generation would be the next iteration.

For Nova Retail's actual production volumes (~500K rows currently), the stock run is the authoritative source of analytical truth. The scaled run answers the question *"if data grows to 5M or 10M rows, does this pipeline hold up?"* — and the answer is yes.

### What 2x Validates About the Architecture

Even with the join-multiplication caveat, the 2x run demonstrates real architectural strength:

- **PySpark's distributed transform engine handled 7.5M rows** through the medallion without intervention
- **The caching strategy paid off**: Gold built three tables from a 7.5M-row cached `fact_sales` in 59 seconds total
- **The Bronze partitioning scaled linearly**: 2x volume produced ~2x Bronze write time (proportional, not exponential)
- **The JDBC load completed**: 7.5M rows over a single JDBC connection took ~5 minutes to write and ~3.5 minutes to validate
- **Row-count validation passed**: 7,533,104 rows in, 7,533,104 rows in PostgreSQL, on every Gold table

The pipeline didn't just survive 2x scaling. It produced correct, validated results.

---

# 🐳 Infrastructure & Deployment

### Docker Compose Service Topology

```yaml
services:
  retail_postgres:        # postgres:15-alpine, persistent volume
  spark:                  # Custom image, sleep infinity, exec'd by Airflow
  airflow-init:           # DB upgrade + admin user creation, one-shot
  airflow-webserver:      # UI on :8080
  airflow-scheduler:      # Triggers DAG runs
```

Five services. All wired together by `docker-compose.yml`. PostgreSQL has a health check (`pg_isready`) that gates dependent services from starting until the database is genuinely accepting connections. The Spark container mounts the `etl/` source code and `data/` directory from the host as volumes, so iterating on Python code doesn't require rebuilding the image.

### Custom Spark Image (`spark.Dockerfile`)

```dockerfile
FROM apache/spark:4.1.1
USER root
RUN pip install --no-cache-dir python-dotenv psycopg2-binary pandas numpy
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/spark/jars/
```

Four lines. Installs the Python dependencies the pipeline needs and bakes the PostgreSQL JDBC driver into Spark's classpath at build time. No runtime downloads. The image is the contract.

### Airflow DAG with Parameterized Execution

The DAG (`airflow/dags/retail_etl_pipeline.py`) supports two parameters via Airflow's `Param` API:

- `scale_data` (boolean, default `false`): enables row-replication scaling for architecture stress testing
- `scale_multiplier` (integer 1-5, default `2`): scaling factor when scale_data is enabled

Scheduled runs (cron `0 6 * * *`, daily at 06:00 UTC) use the defaults — stock data, analytical truth. Manual triggers via the Airflow UI surface a form letting the operator request a scaled run. Both modes flow through the same DAG, the same Spark container, the same pipeline code. The only thing that changes is the environment variables passed through `docker exec`.

### The Sidecar Spark Pattern

Airflow doesn't run Spark itself. Instead:

1. The Spark container runs `sleep infinity` and stays alive as a long-lived execution environment
2. When the DAG triggers, the `BashOperator` calls `docker exec spark /opt/spark/bin/spark-submit ...`
3. Spark runs the job inside its own container with all dependencies already present
4. The job completes; Airflow logs the result; the container stays alive for the next trigger

This pattern keeps orchestration decoupled from execution. Airflow can restart, redeploy, or scale independently of Spark. The same shape works against managed Spark clusters (Databricks, EMR, Dataproc) — `docker exec` becomes a cluster-submit API call, but the orchestration topology is identical.

---

# 🔑 Key Engineering Decisions

### 1️⃣ Why three idempotency layers all use `mode="overwrite"`?

At Nova Retail's volumes (~500K rows currently, ~10M projected ceiling), full rebuilds on every layer cost minutes, not hours. The simplicity of "every run produces the same warehouse state from the same source data" is worth far more than the marginal performance gain from incremental loads. If volumes ever grow past ~50M Silver rows, partition-aware updates would be the natural next step — but the architecture supports that change without rewriting the medallion separation.

### 2️⃣ Why year/month partitioning on facts but not dimensions?

Partition pruning only pays off at volume. `orders` and `order_items` are the tables that grow as the business grows; partitioning them by year/month means a query for "last month's sales" reads only one partition instead of all 24+. Dimension tables (`customers`, `sellers`, `products`) are bounded by business reality (you have only so many sellers) and would just produce directory clutter if partitioned. The honest call: partition where pruning helps, leave alone where it doesn't.

### 3️⃣ Why explicit Spark schemas instead of `inferSchema=True`?

Inference does a full extra pass over each CSV to guess types. Seven CSVs = seven extra full reads on every extract run. Explicit schemas eliminate this. They also pin types correctly: the `review_score` column would be inferred as string (because some rows are NULL) but with explicit schema it reads as integer, the way analytics actually need it. The cost is one-time schema declaration; the benefit is permanent.

### 4️⃣ Why broadcast some dimensions and not others?

Broadcasting ships the small side to every executor instead of shuffling both sides through the network. It's a win when the broadcast side is small enough to fit in executor memory. `sellers` at 3K rows is always safe to broadcast. `products` at 33K is safe. `customers` at 99K rows was on the edge — testing at 5x scale (customers becomes 500K rows) caused JVM memory pressure, so the customers broadcast was removed. The remaining broadcasts on `products` and `sellers` are stable across all tested scales. The discipline: broadcast where the small side stays small, shuffle where it might grow.

### 5️⃣ Why three Gold tables instead of one?

Different queries have different optimal grains. `fact_sales` answers questions about individual transactions. `sales_summary` answers "how does category X perform across states." `sales_by_month_state` answers "how did state Y trend over time." Building these as separate tables with their own primary keys and indexes lets each query hit the right table without scanning the wrong grain. The cost is three writes instead of one. The benefit is queries that run in milliseconds against the right table instead of seconds against a generic fact table.

### 6️⃣ Why row-count validation after every load?

JDBC writes can fail silently. A connection can drop mid-batch. A constraint violation can roll back an insert without erroring at the driver level. A primary key conflict can produce a "succeeded but inserted zero rows" outcome. Validation via read-back row count catches all of these. The pattern: write, read back the count, compare. Same composite-key discipline the load layer dedupes against. If something went wrong, the validation says so loudly. The cost is one extra query per load. The benefit is detecting silent failures before they become operational incidents.

### 7️⃣ Why Airflow parameterization for stock vs scaled?

Production scheduled runs need to produce analytical truth. Architecture validation needs different volumes. Two DAGs would mean two places to maintain the same pipeline-trigger logic. One DAG with parameters means the same orchestration code, two operating modes, controlled at trigger time. The pattern matches what real Airflow deployments do for backfills, replays, and operational what-ifs.

### 8️⃣ Why the sidecar Spark pattern instead of running Spark inside Airflow?

Airflow's `SparkSubmitOperator` requires Spark to be installed inside the Airflow worker container. That couples Airflow's lifecycle to Spark's, which means upgrading one means rebuilding both. The sidecar pattern keeps them independent: the Spark container can be rebuilt with a new Spark version while Airflow stays exactly as it was. This is the same pattern used at scale when Airflow triggers Spark jobs on a remote cluster — the only difference is `docker exec` vs a cluster-submit API call.

### 9️⃣ Why DDL owned by `sql/create_tables.sql`, not auto-created by Spark?

Spark's JDBC writer can auto-create tables, but it auto-infers types (often suboptimally) and silently discards PRIMARY KEYs, FOREIGN KEYs, and indexes entirely. By owning the schema in version-controlled SQL, the warehouse gets full control over composite primary keys, NOT NULL constraints, DECIMAL precision, and read-optimized indexes. The pipeline is a pure data-movement layer; the warehouse is a designed artifact. The DDL auto-runs on first PostgreSQL container start via the `/docker-entrypoint-initdb.d` mount, so the schema exists before any data lands.

### 🔟 Why a custom Spark image instead of pulling stock?

The stock `apache/spark:4.1.1` image doesn't include the PostgreSQL JDBC driver. Without it, every pipeline run would have to download the driver at runtime (slow, network-dependent, version-drift-prone) or mount it from the host (fragile). Baking the driver into a custom image makes it part of the image contract: this image always has the right JDBC driver at the right version. The Dockerfile is four lines. The reliability gain is permanent.

---

# 📊 The Analytics Layer (`sql/queries.sql`)

The warehouse serves 8 production analytical queries answering the business questions Nova Retail's brief identified:

1. **Total business overview**: orders, revenue, average order value, shipping cost as percent of revenue
2. **Monthly sales trend with growth rate**: window-function-based month-over-month growth percentages
3. **Best and worst performing states**: state-level revenue with percentage share via window functions
4. **Top 10 best-selling categories**: category-level aggregation with average order value
5. **Categories with below-average order value**: subquery against `fact_sales` to identify underperforming categories
6. **Top category per state**: partitioned ranking via `RANK() PARTITION BY`
7. **Payment method popularity**: usage counts plus per-state preferred method via window functions
8. **Sales by day of week**: temporal aggregation with day-name CASE expression

The queries mix the three Gold tables appropriately: `fact_sales` for transaction-level questions, `sales_summary` for category-state breakdowns, `sales_by_month_state` for time-series. Each query runs in milliseconds against indexed PostgreSQL tables.

---

# 🛠 Tech Stack

### Distributed Computing
- **PySpark 4.1.1**: distributed transform engine, the medallion implementation
- **Apache Spark**: runtime; custom containerized image
- **Kryo Serializer**: tuned with 1024MB buffer for the 7-way join

### Infrastructure
- **Docker + Docker Compose**: 5-service containerized stack
- **PostgreSQL 15-alpine**: warehouse + Airflow metadata DB
- **PostgreSQL JDBC Driver 42.7.4**: baked into custom Spark image
- **Apache Airflow 2.9.2**: orchestration with parameterized DAGs

### Python Layer
- **psycopg2-binary**: PostgreSQL driver
- **pandas**: small-data manipulation in load validation
- **python-dotenv**: env-var loading with dual-host detection
- **SQLAlchemy**: kept for ad-hoc database work

### Patterns
- **Medallion Architecture**: Bronze/Silver/Gold layered Parquet + warehouse
- **Sidecar Spark**: Airflow `docker exec` into long-lived Spark container
- **Year/Month Partitioning**: storage-layout metadata for read pruning
- **Broadcast Joins**: explicit hints on bounded-size dimensions
- **Three-Layer Idempotency**: overwrite semantics throughout
- **Row-Count Validation**: read-back verification on every load

---

# ▶️ Running the Pipeline

### 1. Bring up the stack
```bash
docker-compose up -d
```
This starts PostgreSQL, Spark, and all three Airflow services. PostgreSQL's DDL auto-runs from `sql/create_tables.sql` on first start.

### 2. Trigger via Airflow UI
Open http://localhost:8080 (admin/admin in development). Click the `retail_etl_pipeline` DAG. Use "Trigger DAG w/ config" to surface the params form:
- Leave `scale_data` unchecked for a stock run
- Check `scale_data` and set `scale_multiplier` to 2-5 for architecture validation

### 3. Or run locally (no Airflow needed)
```powershell
# Stock run (analytical truth)
python -m etl.run_pipeline

# Scaled run (architecture validation)
$env:SCALE_DATA="true"; python -m etl.run_pipeline

# Scaled run with custom multiplier
$env:SCALE_DATA="true"; $env:SCALE_MULTIPLIER="3"; python -m etl.run_pipeline
```

### 4. Inspect the warehouse
```bash
docker exec -it retail_postgres psql -U postgres -d nova_retail_db
\dt retail_gold.*
SELECT * FROM retail_gold.sales_summary ORDER BY total_revenue DESC LIMIT 10;
```

---

# 🔮 Future Iterations

Honest gaps and what I'd ship next:

- **Parallel JDBC writes** via Spark's `numPartitions` + `partitionColumn` to address the load layer becoming the dominant cost at 2x scale and above
- **Cross-platform Spark paths** in `spark_session.py` (currently Windows-tuned hardcoded paths; should be env-driven for full cluster deployment)
- **Incremental Silver via dynamic partition overwrite** so growing data doesn't rebuild the entire Silver layer on every run
- **Spark cluster deployment** (Databricks, EMR, Dataproc) — the architecture is shaped for this; only `spark.master` and infrastructure config would change
- **dbt models on top of Gold** for analyst-authored transformations and tested SQL
- **Migration tooling** (Alembic, Flyway) for evolving DDL safely instead of DROP + CREATE
- **A `dim_*` lookup table layer** for normalized dimension references, currently denormalized into `fact_sales`
- **Airflow connection management** for credential rotation instead of `.env`-based config inside containers

---

# 🧪 Honest Limitations

Naming what this project doesn't do, listed straight:

- **No automated tests.** Testing was via real pipeline runs and log inspection.
- **Single-node Spark.** Tuned for a 16GB+ developer laptop; cluster deployment would change the master URL and memory config.
- **Windows-tuned Spark paths.** `C:/hadoop` and `C:/spark/jars/...` are hardcoded in `spark_session.py` for the bare-metal developer workflow. Inside the Docker container, Spark uses its own installation, so the containerized Airflow-triggered runs work cross-platform; the local-development standalone runs are Windows-bound.
- **The Spark shutdown error at end of every Windows run.** Java HotSpot occasionally fails to delete its temp directory due to file-handle release timing. This is a known Spark-on-Windows cosmetic issue, not a pipeline bug. The pipeline always succeeds before the shutdown hook runs.
- **Single-threaded JDBC writes.** At 2x scale, fact_sales took ~5 minutes to load over a single JDBC connection. At higher scales, parallel JDBC writes via `numPartitions` would be the next optimization.
- **Row-replication scaling produces join multiplication.** Documented honestly in the Scaling Methodology section. 2x source data produces 7.5M Silver records due to duplicate-key join multiplication. This validates throughput, not analytical correctness at scale.
- **`requirements.txt` partially pinned.** Only PySpark is version-locked. For full reproducibility, `pip-tools` or `poetry` would lock the dependency tree.
- **Airflow uses the same PostgreSQL instance as the warehouse.** Different databases (`nova_retail_db` for analytics, separate metadata DB for Airflow), but on the same Postgres container. Acceptable for development; production would separate these.

---

# 🤝 Contributing

If you'd like to contribute, feel free to:

1. Fork the repo
2. Create a feature branch
3. Commit changes
4. Open a pull request

---

# 📄 License

This project is released under the **MIT License**.

---

# 👤 Author

**Yomi Ismail**
Data Engineer · Suffolk, UK
[LinkedIn](https://www.linkedin.com/in/yomi-ismail) · [GitHub](https://github.com/yoismail) · [Portfolio](https://yoismail.github.io/portfolio/)

[![GitHub](https://img.shields.io/badge/GitHub-yoismail-black?logo=github)](https://github.com/yoismail)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-yomi--ismail-blue?logo=linkedin)](https://www.linkedin.com/in/yomi-ismail/)
```
