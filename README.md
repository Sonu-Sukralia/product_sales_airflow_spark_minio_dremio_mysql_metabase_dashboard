# Modern Data Platform with Medallion Architecture

A comprehensive data engineering platform implementing the Medallion Architecture (Bronze-Silver-Gold) using Apache Spark, Airflow, MinIO, and modern analytics tools.

![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Spark](https://img.shields.io/badge/Apache-Spark-E25A1C)
![Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE)

## 🏗️ Architecture Overview

This platform implements a modern data lakehouse architecture with the following layers:

```
┌─────────────────────────────────────────────────────────────┐
│                     PRESENTATION LAYER                       │
│  Metabase (BI) │ Dremio (Query Engine) │ Jupyter Notebooks  │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│                      GOLD LAYER (Analytics)                  │
│  • Daily Aggregates     • Monthly Aggregates                │
│  • Product Analytics    • Region Analytics                  │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Validated)                   │
│  • Data Quality Checks (Deequ)                              │
│  • Data Cleansing & Standardization                         │
│  • Business Rules Application                                │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Data)                   │
│  • Raw Ingestion        • Schema Preservation                │
│  • Historical Archive   • Source System Metadata            │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Key Features

### Data Processing
- **Medallion Architecture**: Bronze → Silver → Gold data transformation pipeline
- **Apache Spark**: Distributed data processing with PySpark
- **Data Quality**: Validation using Amazon Deequ
- **Orchestration**: Automated workflows with Apache Airflow

### Storage & Catalog
- **MinIO**: S3-compatible object storage for data lakes
- **Nessie**: Git-like version control for data
- **Partitioning**: Date-based partitioning for efficient queries

### Analytics & Visualization
- **Dremio**: Interactive SQL query engine with semantic layer
- **Metabase**: Business intelligence dashboards
- **Jupyter**: Interactive notebooks for data exploration

### Infrastructure
- **Containerized**: Fully dockerized with Docker Compose
- **Scalable**: Spark cluster with master-worker architecture
- **Monitored**: Spark History Server for job tracking

## 📦 Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Processing | Apache Spark 3.x | Distributed data processing |
| Orchestration | Apache Airflow 2.x | Workflow automation |
| Storage | MinIO | S3-compatible object storage |
| Catalog | Nessie | Data version control |
| Query Engine | Dremio | Interactive SQL analytics |
| BI Tool | Metabase | Business dashboards |
| Notebooks | Jupyter | Interactive analysis |
| Databases | PostgreSQL, MySQL | Metadata & analytics |

## 🛠️ Prerequisites

- Docker Desktop (20.10+)
- Docker Compose (2.0+)
- 8GB+ RAM recommended
- 20GB+ disk space

## 📋 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd data-platform
```

### 2. Configure Environment Variables

Create `.env` file:

```bash
# MinIO Configuration
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password

# PostgreSQL Configuration
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# MySQL Configuration
MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_DATABASE=analytics
MYSQL_USER=analytics_user
MYSQL_PASSWORD=analytics123
```

Create `airflow.env` file:

```bash
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

### 3. Create Required Directories

```bash
mkdir -p jobs dags notebooks data spark-events spark-logs/master spark-logs/worker \
         spark-logs/history spark-work postgres mysql minio nessie dremio \
         airflow-logs metabase-data spark-conf mysql-init
```

### 4. Create Spark Configuration

Create `spark-conf/spark-defaults.conf`:

```properties
spark.master                     spark://spark-master:7077
spark.eventLog.enabled          true
spark.eventLog.dir              /opt/spark-events
spark.history.fs.logDirectory   /opt/spark-events
spark.sql.adaptive.enabled      true
spark.sql.adaptive.coalescePartitions.enabled true
```

### 5. Create MySQL Initialization Script

Create `mysql-init/01-init.sql`:

```sql
-- Create analytics database for Metabase
CREATE DATABASE IF NOT EXISTS analytics;

-- Create metabase user if needed
CREATE USER IF NOT EXISTS 'metabase'@'%' IDENTIFIED BY 'metabase123';
GRANT ALL PRIVILEGES ON analytics.* TO 'metabase'@'%';
GRANT ALL PRIVILEGES ON metabase.* TO 'metabase'@'%';

-- Grant privileges to root for analytics
GRANT ALL PRIVILEGES ON analytics.* TO 'root'@'%';

FLUSH PRIVILEGES;

USE analytics;

-- Daily aggregates table
CREATE TABLE IF NOT EXISTS sales_daily_aggregates (
    transaction_date DATE,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    region VARCHAR(50),
    total_quantity_sold INT,
    total_revenue DECIMAL(15,2),
    avg_unit_price DECIMAL(10,2),
    avg_discount_percent DECIMAL(5,2),
    transaction_count INT,
    max_transaction_amount DECIMAL(10,2),
    min_transaction_amount DECIMAL(10,2),
    revenue_rank INT,
    report_date DATE,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date (transaction_date),
    INDEX idx_product (product_name),
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Monthly aggregates table
CREATE TABLE IF NOT EXISTS sales_monthly_aggregates (
    year INT,
    month INT,
    month_name VARCHAR(20),
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    total_quantity_sold INT,
    total_revenue DECIMAL(15,2),
    avg_unit_price DECIMAL(10,2),
    transaction_count INT,
    unique_customers_approx INT,
    revenue_rounded DECIMAL(15,2),
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_year_month (year, month),
    INDEX idx_product (product_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Product analytics table
CREATE TABLE IF NOT EXISTS sales_product_analytics (
    product_name VARCHAR(100) PRIMARY KEY,
    product_category VARCHAR(50),
    lifetime_revenue DECIMAL(15,2),
    lifetime_quantity_sold INT,
    avg_selling_price DECIMAL(10,2),
    avg_discount DECIMAL(5,2),
    total_transactions INT,
    total_customers_approx INT,
    lifetime_revenue_rounded DECIMAL(15,2),
    revenue_rank INT,
    market_share_percent DECIMAL(5,2),
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_category (product_category),
    INDEX idx_revenue_rank (revenue_rank)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 6. Start the Platform

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 7. Access the Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8085 | admin / admin |
| Spark Master UI | http://localhost:9090 | - |
| Spark Worker UI | http://localhost:8081 | - |
| Spark History | http://localhost:18080 | - |
| MinIO Console | http://localhost:9001 | admin / password |
| Dremio | http://localhost:9047 | (first-time setup) |
| Metabase | http://localhost:3000 | (first-time setup) |
| Jupyter | http://localhost:8888 | (check logs for token) |

**To get Jupyter token:**
```bash
docker logs jupyter-spark 2>&1 | grep "token="
```

## 📊 Data Pipeline

### Bronze Layer (Raw Data Ingestion)

**Location**: `jobs/bronze_layer.py`

Generates realistic product sales data with intentional quality issues for testing:

- **Products**: 11 categories (laptop, mobile, mouse, tv, hardisk, ram, speaker, watch, keyboard, mic, camera)
- **Volume**: 50-500 transactions per product per day
- **Data Quality**: 5% intentional issues for validation testing
- **Fields**: transaction_id, product_name, quantity, unit_price, discount, customer_id, store_id, payment_method, region

**Sample Output:**
```
📊 Total Records: 5,432
💾 Data written to: s3a://bronze/sales/date=2025-01-19
```

### Silver Layer (Data Validation & Cleansing)

**Location**: `jobs/silver_layer.py`

Validates and cleanses data using manual checks and Amazon Deequ:

**Quality Checks:**
- ✅ Null value analysis
- ✅ Negative value validation
- ✅ Schema validation
- ✅ Pattern matching (customer_id: CUST####, store_id: STORE###)
- ✅ Data standardization (uppercase, trimming)
- ✅ Business rule validation

**Quality Report Example:**
```json
{
  "total_records": 5432,
  "quality_score": 95.2,
  "issues": {
    "negative_quantity": 12,
    "negative_price": 8,
    "empty_product_name": 15
  }
}
```

**Sample Output:**
```
📊 Records before cleaning: 5,432
✅ Records after cleaning: 5,165
🗑️  Records removed: 267 (4.91%)
```

### Gold Layer (Business Aggregations)

**Location**: `jobs/gold_layer.py`

Creates analytics-ready datasets:

**1. Daily Aggregates**
- Total revenue by product/region
- Transaction counts and rankings
- Average prices and discounts
- Min/max transaction amounts

**2. Monthly Aggregates**
- Month-over-month trends
- Product performance by month
- Customer engagement metrics

**3. Product Analytics**
- Lifetime revenue and quantity
- Market share percentage
- Performance rankings
- Average selling price

**Sample Output:**
```
🏆 TOP 10 PRODUCTS BY REVENUE:
+------+----------+------------------+---------------------+------------------+
| rank | product  | lifetime_revenue | lifetime_quantity   | market_share_%   |
+------+----------+------------------+---------------------+------------------+
| 1    | CAMERA   | 12,456,789.50   | 8,234              | 18.5             |
| 2    | LAPTOP   | 9,876,543.20    | 6,543              | 14.7             |
+------+----------+------------------+---------------------+------------------+
```

### Load to MySQL (Metabase Integration)

**Location**: `jobs/load_to_mysql.py`

Loads gold layer data into MySQL for BI tools:

- **Table**: `sales_daily_aggregates` - Daily metrics
- **Table**: `sales_monthly_aggregates` - Monthly trends
- **Table**: `sales_product_analytics` - Product insights

## 🔄 Running the Pipeline

### Option 1: Via Airflow (Recommended)

1. Access Airflow UI: http://localhost:8085
2. Login with: `admin` / `admin`
3. Find DAG: `sales_medallion_pipeline`
4. Toggle the DAG to ON
5. Click "Trigger DAG" to run manually
6. Monitor execution in the Graph or Tree view

**DAG Schedule**: Daily at 2:00 AM UTC (configurable)

### Option 2: Manual Spark Job Submission

```bash
# Bronze Layer - Generate Data
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/bronze_layer.py \
  --date 2025-01-19 \
  --output-path s3a://bronze/sales/date=2025-01-19

# Silver Layer - Validate & Clean
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/silver_layer.py \
  --date 2025-01-19 \
  --input-path s3a://bronze/sales/date=2025-01-19 \
  --output-path s3a://silver/sales/date=2025-01-19 \
  --quality-output s3a://silver/quality-reports/date=2025-01-19

# Gold Layer - Aggregate
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/gold_layer.py \
  --date 2025-01-19 \
  --input-path s3a://silver/sales/date=2025-01-19 \
  --output-path-daily s3a://gold/daily-aggregates/date=2025-01-19 \
  --output-path-monthly s3a://gold/monthly-aggregates \
  --output-path-product s3a://gold/product-analytics

# Load to MySQL
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages mysql:mysql-connector-java:8.0.33 \
  /opt/bitnami/spark/jobs/load_to_mysql.py \
  --date 2025-01-19 \
  --gold-daily-path s3a://gold/daily-aggregates/date=2025-01-19 \
  --gold-monthly-path s3a://gold/monthly-aggregates \
  --gold-product-path s3a://gold/product-analytics \
  --mysql-host mysql \
  --mysql-port 3306 \
  --mysql-database analytics \
  --mysql-user root \
  --mysql-password rootpassword
```

### Option 3: Interactive with Jupyter

1. Access Jupyter: http://localhost:8888
2. Get token from logs: `docker logs jupyter-spark 2>&1 | grep "token="`
3. Create new notebook
4. Run PySpark code interactively

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Interactive Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read gold data
df = spark.read.parquet("s3a://gold/product-analytics")
df.show()
```

## 📈 Monitoring & Observability

### Spark Monitoring

**Spark Master UI** (http://localhost:9090)
- Worker status and resources
- Running applications
- Completed applications

**Spark History Server** (http://localhost:18080)
- Job execution timeline
- Stage and task metrics
- Event logs and DAG visualization

**Worker UI** (http://localhost:8081)
- Executor status
- Running tasks
- Log files

### Airflow Monitoring

**Airflow Web UI** (http://localhost:8085)
- DAG runs and task status
- Gantt charts for timing
- Task logs for debugging
- XCom for inter-task communication

### MinIO Monitoring

**MinIO Console** (http://localhost:9001)
- Bucket usage and objects
- Access logs
- Performance metrics

### Data Quality Metrics

Quality reports are stored in: `s3a://silver/quality-reports/date=YYYY-MM-DD/`

```json
{
  "total_records": 5432,
  "quality_score": 95.2,
  "issues": {
    "null_values": {
      "product_name": 0,
      "quantity": 0,
      "total_amount": 15
    },
    "negative_quantity": 12,
    "negative_price": 8,
    "empty_product_name": 15
  }
}
```

## 📁 Directory Structure

```
data-platform/
├── docker-compose.yml              # Service orchestration
├── .env                            # Environment variables
├── airflow.env                     # Airflow configuration
├── README.md                       # This file
│
├── jobs/                           # Spark jobs (PySpark)
│   ├── bronze_layer.py             # Data generation & ingestion
│   ├── silver_layer.py             # Validation & cleansing
│   ├── gold_layer.py               # Business aggregations
│   └── load_to_mysql.py            # MySQL loader
│
├── dags/                           # Airflow DAGs
│   └── sales_medallion_dag.py      # Pipeline orchestration
│
├── notebooks/                      # Jupyter notebooks
│   └── analysis.ipynb              # Example analysis
│
├── spark-conf/                     # Spark configuration
│   └── spark-defaults.conf         # Spark settings
│
├── mysql-init/                     # MySQL initialization
│   └── 01-init.sql                 # Table creation scripts
│
├── data/                           # Shared data directory
│
├── spark-events/                   # Spark event logs
├── spark-logs/                     # Spark service logs
│   ├── master/
│   ├── worker/
│   └── history/
│
├── spark-work/                     # Spark working directory
├── postgres/                       # PostgreSQL data
├── mysql/                          # MySQL data
├── minio/                          # MinIO data
├── nessie/                         # Nessie data
├── dremio/                         # Dremio data
├── airflow-logs/                   # Airflow logs
└── metabase-data/                  # Metabase data
```

## 🔧 Configuration

### Spark Configuration

Edit `spark-conf/spark-defaults.conf`:

```properties
# Master URL
spark.master                                spark://spark-master:7077

# Event Logging
spark.eventLog.enabled                      true
spark.eventLog.dir                          /opt/spark-events
spark.history.fs.logDirectory               /opt/spark-events

# Adaptive Query Execution
spark.sql.adaptive.enabled                  true
spark.sql.adaptive.coalescePartitions.enabled true

# Memory Configuration
spark.executor.memory                       4g
spark.driver.memory                         2g

# Shuffle Configuration
spark.sql.shuffle.partitions                200
spark.default.parallelism                   100

# Network Timeout
spark.network.timeout                       300s
```

### Scaling Spark Workers

Add more workers by editing `docker-compose.yml`:

```yaml
spark-worker-2:
  image: sonusukralia/v3_spark_mw:1.0
  container_name: spark-worker-2
  hostname: spark-worker-2
  user: root
  depends_on: [spark-master]
  command: >
    bash -c "
    exec /opt/bitnami/spark/sbin/start-worker.sh spark://spark-master:7077
    "
  ports:
    - "8082:8081"  # Different port for UI
  environment:
    SPARK_MODE: worker
    SPARK_MASTER_URL: spark://spark-master:7077
    SPARK_WORKER_WEBUI_PORT: "8081"
    # ... rest of configuration same as spark-worker
  volumes:
    - ./jobs:/opt/bitnami/spark/jobs
    - ./spark-events:/opt/spark-events
    - ./spark-logs/worker-2:/opt/bitnami/spark/logs
  networks: [data-platform-network]
  restart: unless-stopped
```

Then restart: `docker-compose up -d`

### Airflow Configuration

Edit `airflow.env` for Airflow settings:

```bash
# Executor Type
AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Database Connection
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow

# DAG Configuration
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

# Webserver
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
AIRFLOW__WEBSERVER__RBAC=True

# Scheduler
AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=False
```

## 🎯 Use Cases

### 1. Daily Sales Analytics Dashboard

**Goal**: Track daily sales performance across products and regions

**Steps**:
1. Run the medallion pipeline (generates data through gold layer)
2. Data is automatically loaded to MySQL
3. Connect Metabase to MySQL (analytics database)
4. Create dashboard with:
   - Total daily revenue
   - Top 10 products by revenue
   - Regional sales comparison
   - Transaction count trends

**Metabase Setup**:
```
Database Type: MySQL
Host: mysql
Port: 3306
Database: analytics
Username: root
Password: rootpassword

Tables Available:
- sales_daily_aggregates
- sales_monthly_aggregates
- sales_product_analytics
```

### 2. Data Quality Monitoring

**Goal**: Monitor and track data quality metrics over time

**Steps**:
1. Silver layer generates quality reports daily
2. Reports stored in: `s3a://silver/quality-reports/`
3. Read reports in Jupyter or Dremio
4. Track quality score trends
5. Set up alerts for quality degradation

**Quality Metrics Tracked**:
- Null value percentage
- Negative value count
- Schema validation failures
- Pattern matching failures
- Overall quality score (0-100)

### 3. Product Performance Analysis

**Goal**: Analyze product performance and market share

**Steps**:
1. Query `sales_product_analytics` table
2. Analyze lifetime revenue, quantity sold
3. Compare market share across products
4. Identify top performers and underperformers

**Sample Query in Dremio**:
```sql
SELECT 
    product_name,
    product_category,
    lifetime_revenue_rounded,
    market_share_percent,
    revenue_rank
FROM analytics.sales_product_analytics
ORDER BY revenue_rank
LIMIT 10;
```
## Metabase – Top 10 Products Analysis

This dashboard shows the top 10 products ranked by lifetime revenue and quantity,
generated from MySQL analytics tables and visualized using Metabase.

![Top 10 Products Dashboard](images/metabase_top_10_products.png)

### 4. Interactive Data Exploration

**Goal**: Ad-hoc analysis and data exploration

**Steps**:
1. Open Jupyter: http://localhost:8888
2. Connect to Spark cluster
3. Read data from MinIO (bronze/silver/gold)
4. Perform custom analysis
5. Create visualizations

**Example Notebook**:
```python
# Read product analytics
df = spark.read.parquet("s3a://gold/product-analytics")

# Find products with >10% market share
high_performers = df.filter(col("market_share_percent") > 10)

# Visualize with matplotlib
import matplotlib.pyplot as plt
pandas_df = high_performers.toPandas()
pandas_df.plot(kind='bar', x='product_name', y='lifetime_revenue')
plt.show()
```

## 🐛 Troubleshooting

### Services Not Starting

```bash
# Check all services
docker-compose ps

# View logs for specific service
docker-compose logs -f spark-master
docker-compose logs -f postgres
docker-compose logs -f airflow-scheduler

# Restart specific service
docker-compose restart spark-master

# Rebuild and restart
docker-compose down
docker-compose up -d --build
```

### Common Issues

#### 1. Spark Master Not Accessible

**Symptom**: Worker can't connect to master
```
ERROR Worker: Failed to connect to master
```

**Solution**:
```bash
# Check master is running
docker-compose ps spark-master

# Check logs
docker-compose logs spark-master

# Restart master
docker-compose restart spark-master

# Verify network
docker network inspect data-platform-network
```

#### 2. MinIO Buckets Not Created

**Symptom**: S3 access errors
```
NoSuchBucket: The specified bucket does not exist
```

**Solution**:
```bash
# Check minio-mc logs
docker-compose logs minio-mc

# Manually create buckets
docker exec -it minio-mc /bin/sh
mc alias set minio http://minio:9000 admin password
mc mb minio/bronze minio/silver minio/gold
mc policy set public minio/bronze minio/silver minio/gold
```

#### 3. Airflow Database Not Initialized

**Symptom**: Airflow webserver crashes
```
airflow.exceptions.AirflowException: Database not initialized
```

**Solution**:
```bash
# Stop services
docker-compose down

# Remove postgres data
rm -rf postgres/

# Start postgres only
docker-compose up -d postgres

# Wait for postgres to be healthy
docker-compose ps postgres

# Start airflow services
docker-compose up -d airflow-scheduler airflow-webserver
```

#### 4. Out of Memory Errors

**Symptom**: Spark jobs failing with OOM
```
java.lang.OutOfMemoryError: Java heap space
```

**Solution**:
```bash
# Increase executor memory in spark-conf/spark-defaults.conf
spark.executor.memory    8g
spark.driver.memory      4g

# Or reduce data partitions
spark.sql.shuffle.partitions    100

# Restart Spark cluster
docker-compose restart spark-master spark-worker
```

#### 5. MySQL Connection Refused

**Symptom**: Cannot connect to MySQL
```
Can't connect to MySQL server on 'mysql'
```

**Solution**:
```bash
# Check MySQL is running
docker-compose ps mysql

# Check MySQL logs
docker-compose logs mysql

# Test connection
docker exec -it mysql mysql -u root -prootpassword -e "SHOW DATABASES;"

# Verify port is exposed
docker port mysql 3306
```

#### 6. Jupyter Kernel Not Starting

**Symptom**: Kernel connection errors

**Solution**:
```bash
# Restart Jupyter
docker-compose restart jupyter-spark

# Check logs for errors
docker-compose logs jupyter-spark

# Get fresh token
docker logs jupyter-spark 2>&1 | grep "token="
```

### Debug Commands

```bash
# Enter container shell
docker exec -it spark-master bash
docker exec -it airflow-scheduler bash

# Check Spark cluster
docker exec -it spark-master cat /opt/bitnami/spark/logs/master.out

# Check MinIO buckets
docker exec -it minio-mc mc ls minio/

# Check MySQL databases
docker exec -it mysql mysql -u root -prootpassword -e "SHOW DATABASES;"

# Check Airflow database
docker exec -it postgres psql -U airflow -c "\dt"

# View resource usage
docker stats

# Check disk space
docker system df
```

### Performance Debugging

```bash
# Check Spark job execution
# Visit: http://localhost:9090 (Master UI)
# Visit: http://localhost:18080 (History Server)

# Check Airflow task logs
# Visit: http://localhost:8085
# Navigate to DAG > Task Instance > Log

# Monitor container resources
docker stats --no-stream

# Check MinIO performance
# Visit: http://localhost:9001 (MinIO Console)
```

## 🔐 Security Considerations

⚠️ **This setup is for DEVELOPMENT/TESTING only. For PRODUCTION, implement these security measures:**

### 1. Change Default Credentials

```bash
# Update .env file
MINIO_ROOT_USER=your_secure_username
MINIO_ROOT_PASSWORD=your_secure_password_min_8_chars

MYSQL_ROOT_PASSWORD=your_secure_mysql_password
POSTGRES_PASSWORD=your_secure_postgres_password

# Update airflow.env
AIRFLOW_ADMIN_PASSWORD=your_secure_airflow_password
```

### 2. Enable SSL/TLS

For MinIO:
```yaml
# docker-compose.yml
minio:
  environment:
    MINIO_SERVER_URL: "https://minio.yourdomain.com"
  volumes:
    - ./certs:/root/.minio/certs
```

For Dremio and Metabase: Configure SSL certificates in respective configurations.

### 3. Use Secrets Management

```yaml
# docker-compose.yml
services:
  postgres:
    secrets:
      - postgres_password
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password

secrets:
  postgres_password:
    file: ./secrets/postgres_password.txt
```

### 4. Network Segmentation

```yaml
networks:
  frontend:
    driver: bridge
  backend:
    driver: bridge
    internal: true  # No external access
```

### 5. Enable Authentication

- **MinIO**: Enable IAM policies and access keys
- **Dremio**: Configure LDAP/OAuth
- **Metabase**: Enable LDAP/SAML
- **Airflow**: Configure RBAC with custom roles

### 6. Audit Logging

Enable audit logs for all services:
- MinIO audit logging
- Airflow task logs retention
- Spark event logs
- Database query logs

## 🚀 Performance Optimization

### Spark Tuning

**Memory Optimization**:
```properties
# spark-defaults.conf
spark.executor.memory              8g
spark.driver.memory                4g
spark.memory.fraction              0.8
spark.memory.storageFraction       0.3
```

**Parallelism**:
```properties
spark.default.parallelism          200
spark.sql.shuffle.partitions       200
spark.sql.files.maxPartitionBytes  134217728  # 128 MB
```

**Adaptive Query Execution**:
```properties
spark.sql.adaptive.enabled                          true
spark.sql.adaptive.coalescePartitions.enabled       true
spark.sql.adaptive.skewJoin.enabled                 true
spark.sql.adaptive.localShuffleReader.enabled       true
```

### MinIO Best Practices

1. **Use appropriate bucket policies**
2. **Enable versioning for critical data**
3. **Set up lifecycle rules for old data**
4. **Use multi-part upload for large files**

```bash
# Set lifecycle policy (delete after 90 days)
mc ilm add --expiry-days 90 minio/bronze/old-data
```

**Data Partitioning Strategy**:
```python
# Partition by date for better query performance
df.write.partitionBy("year", "month", "day").parquet("s3a://gold/sales/")
```

### Airflow Optimization

**DAG Performance**:
```python
# Use pools to limit concurrency
# In Airflow UI: Admin > Pools

# Use task dependencies wisely
task1 >> [task2, task3]  # Parallel execution

# Enable parallelism
AIRFLOW__CORE__PARALLELISM=32
AIRFLOW__CORE__DAG_CONCURRENCY=16
```

### MySQL Optimization

**Index Strategy**:
```sql
-- Add composite indexes for common queries
CREATE INDEX idx_date_product ON sales_daily_aggregates(transaction_date, product_name);
CREATE INDEX idx_category_revenue ON sales_product_analytics(product_category, lifetime_revenue);

-- Analyze tables regularly
ANALYZE TABLE sales_daily_aggregates;
ANALYZE TABLE sales_monthly_aggregates;
```

**Query Optimization**:
```sql
-- Use EXPLAIN to understand query execution
EXPLAIN SELECT * FROM sales_daily_aggregates WHERE transaction_date > '2025-01-01';

-- Use covering indexes
SELECT product_name, total_revenue 
FROM sales_daily_aggregates 
WHERE transaction_date = '2025-01-19'
ORDER BY total_revenue DESC;
```

## 📚 Additional Resources

### Documentation
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [MinIO Documentation](https://min.io/docs/)
- [Dremio Documentation](https://docs.dremio.com/)
- [Metabase Documentation](https://www.metabase.com/docs/)
- [Amazon Deequ Documentation](https://github.com/awslabs/deequ)

### Tutorials & Guides
- [Medallion Architecture Best Practices](https://www.databricks.com/glossary/medallion-architecture)
- [Data Quality with Deequ](https://aws.amazon.com/blogs/big-data/test-data-quality-at-scale-with-deequ/)
- [PySpark Tutorial](https://spark.apache.org/docs/latest/api/python/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

### Community & Support
- [Apache Spark User Mailing List](https://spark.apache.org/community.html)
- [Airflow Slack Community](https://apache-airflow-slack.herokuapp.com/)
- [Stack Overflow - Spark](https://stackoverflow.com/questions/tagged/apache-spark)
- [Stack Overflow - Airflow](https://stackoverflow.com/questions/tagged/airflow)

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/amazing-feature
   ```
3. **Commit your changes**
   ```bash
   git commit -m 'Add some amazing feature'
   ```
4. **Push to the branch**
   ```bash
   git push origin feature/amazing-feature
   ```
5. **Open a Pull Request**

### Contribution Guidelines

- Follow PEP 8 for Python code
- Add docstrings to all functions
- Update README if adding new features
- Test your changes locally before submitting
- Include screenshots for UI changes

## 📝 License

This project is licensed under the MIT License - see below for details:

```
MIT License

Copyright (c) 2025 [Your Name]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## 🎓 Learning Path

### Beginner Level
1. **Setup the environment** - Follow Quick Start guide
2. **Run the pipeline manually** - Execute each layer step-by-step
3. **Explore the data** - Use Jupyter notebooks
4. **Create simple visualizations** - Build basic Metabase dashboards

### Intermediate Level
1. **Customize the DAG** - Modify schedule and add new tasks
2. **Add new data sources** - Extend bronze layer
3. **Create custom aggregations** - Add business logic to gold layer
4. **Optimize performance** - Tune Spark configurations

### Advanced Level
1. **Implement CDC** - Change Data Capture patterns
2. **Add data versioning** - Use Nessie for data versioning
3. **Build ML pipelines** - Integrate MLflow for model training
4. **Production deployment** - Kubernetes deployment with Helm

## 🏆 Best Practices

### Code Organization
- Keep each layer's code separate and modular
- Use configuration files for environment-specific settings
- Implement proper error handling and logging
- Write unit tests for transformation logic

### Data Management
- Implement proper data retention policies
- Use appropriate partitioning strategies
- Monitor data quality continuously
- Document data lineage

### Operations
- Set up monitoring and alerting
- Implement automated backups
- Use version control for all code
- Maintain comprehensive documentation

### Security
- Never commit credentials to version control
- Use environment variables for secrets
- Implement least privilege access
- Regular security audits

## 📊 Sample Dashboards

### Metabase Dashboard Ideas

**Sales Overview Dashboard**:
- Total Revenue Card
- Transaction Count Card
- Top 10 Products Table
- Revenue Trend Line Chart
- Regional Distribution Pie Chart

**Product Performance Dashboard**:
- Market Share by Product
- Revenue by Category
- Product Ranking Table
- Price vs. Quantity Scatter Plot

**Data Quality Dashboard**:
- Daily Quality Score Line Chart
- Issue Breakdown Pie Chart
- Data Volume Trend
- Processing Time Metrics

## 🔮 Roadmap

### Planned Features
- [ ] Real-time streaming with Kafka
- [ ] Machine learning pipeline integration
- [ ] Advanced data catalog with DataHub
- [ ] Delta Lake integration
- [ ] Kubernetes deployment also done but having some network issue, will fix in future.
- [ ] CI/CD pipeline with GitHub Actions
- [ ] Data observability with Great Expectations
- [ ] Cost optimization dashboard

## 👥 Authors & Acknowledgments

### Authors
- **Your Name** - *Sonu Kumar* - [GitHub](https://github.com/Sonu-Sukralia)

### Acknowledgments
- Apache Software Foundation for Spark and Airflow
- MinIO for S3-compatible object storage
- Dremio for the amazing query engine
- Metabase for intuitive BI tool
- The entire open-source community

## 📞 Support & Contact

### Getting Help
- 📧 **Email**: en.sonukumar@gmail.com
- 💬 **Discussions**: [GitHub Discussions](https://github.com/Sonu-Sukralia)
- 🐛 **Issues**: [GitHub Issues](https://github.com/Sonu-Sukralia)
- 📝 **Documentation**: [Wiki](https://github.com/Sonu-Sukralia)

### Community
- Join our [Slack Channel](#) for real-time help
- Follow updates on [Twitter](#)
- Watch tutorials on [YouTube](#)

## ⭐ Show Your Support

If you find this project helpful, please:
- ⭐ **Star this repository**
- 🍴 **Fork it** for your own use
- 📢 **Share it** with others
- 💬 **Provide feedback** via issues
- 🤝 **Contribute** to make it better

## 📈 Project Status

![GitHub last commit](https://img.shields.io/github/last-commit/yourusername/data-platform)
![GitHub issues](https://img.shields.io/github/issues/yourusername/data-platform)
![GitHub pull requests](https://img.shields.io/github/issues-pr/yourusername/data-platform)
![GitHub stars](https://img.shields.io/github/stars/yourusername/data-platform)
![GitHub forks](https://img.shields.io/github/forks/yourusername/data-platform)

---

*Last Updated: January 2026*