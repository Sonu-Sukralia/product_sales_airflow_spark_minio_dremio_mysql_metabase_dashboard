#!/bin/bash

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Product Sales Analytics Pipeline - Setup Script          ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to print status
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Check if Docker is installed
echo -e "${YELLOW}Checking prerequisites...${NC}"
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi
print_status "Docker is installed"

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi
print_status "Docker Compose is installed"

# Create directory structure
echo ""
echo -e "${YELLOW}Creating directory structure...${NC}"
mkdir -p dags jobs mysql-init data logs spark-events notebooks
mkdir -p spark-conf spark-logs/master spark-logs/worker spark-logs/history spark-logs/jupyter
mkdir -p minio postgres mysql dremio nessie spark-work metabase-data
print_status "Directories created"

# Set permissions
echo ""
echo -e "${YELLOW}Setting permissions...${NC}"
chmod -R 777 logs dags jobs data spark-events spark-logs spark-work 2>/dev/null || true
print_status "Permissions set"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo ""
    echo -e "${YELLOW}Creating .env file...${NC}"
    cat > .env << 'EOF'
# PostgreSQL (Airflow Metadata)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# MySQL (Analytics DB)
MYSQL_ROOT_PASSWORD=rootpassword
MYSQL_DATABASE=analytics
MYSQL_USER=analytics_user
MYSQL_PASSWORD=analytics123

# MinIO (S3 Storage)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password

# Spark
SPARK_MASTER_HOST=spark-master
SPARK_WORKER_MEMORY=2G
SPARK_WORKER_CORES=2
EOF
    print_status ".env file created"
else
    print_info ".env file already exists, skipping"
fi

# Create airflow.env file if it doesn't exist
if [ ! -f airflow.env ]; then
    echo ""
    echo -e "${YELLOW}Creating airflow.env file...${NC}"
    cat > airflow.env << 'EOF'
# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__WEBSERVER__SECRET_KEY=changeme-secret-key-12345

# Admin User
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
EOF
    print_status "airflow.env file created"
else
    print_info "airflow.env file already exists, skipping"
fi

# Create spark-defaults.conf if it doesn't exist
if [ ! -f spark-conf/spark-defaults.conf ]; then
    echo ""
    echo -e "${YELLOW}Creating Spark configuration...${NC}"
    cat > spark-conf/spark-defaults.conf << 'EOF'
spark.master                     spark://spark-master:7077
spark.eventLog.enabled           true
spark.eventLog.dir               /opt/spark-events
spark.history.fs.logDirectory    /opt/spark-events
spark.sql.warehouse.dir          /opt/spark-warehouse

# S3/MinIO Configuration
spark.hadoop.fs.s3a.endpoint     http://minio:9000
spark.hadoop.fs.s3a.access.key   admin
spark.hadoop.fs.s3a.secret.key   password
spark.hadoop.fs.s3a.path.style.access  true
spark.hadoop.fs.s3a.impl         org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled  false
EOF
    print_status "Spark configuration created"
else
    print_info "Spark configuration already exists, skipping"
fi

# Check if docker-compose.yml exists
if [ ! -f docker-compose.yml ]; then
    print_error "docker-compose.yml not found!"
    print_info "Please ensure docker-compose.yml is in the current directory"
    exit 1
fi

# Check if DAG file exists
if [ ! -f dags/product_sales_pipeline.py ]; then
    print_warning "DAG file not found at dags/product_sales_pipeline.py"
    print_info "Make sure to add your DAG file before running the pipeline"
fi

# Check if Spark jobs exist
if [ ! -f jobs/generate_sales_data.py ]; then
    print_warning "Spark job files not found in jobs/ directory"
    print_info "Make sure to add all Spark job scripts before running the pipeline"
fi

# Start services
echo ""
echo -e "${YELLOW}Starting Docker services...${NC}"
docker-compose up -d

echo ""
echo -e "${YELLOW}Waiting for services to start...${NC}"
sleep 10

# Check service health
echo ""
echo -e "${YELLOW}Checking service status...${NC}"

services=("spark-master" "spark-worker" "postgres" "mysql" "minio" "airflow-scheduler" "airflow-webserver" "metabase")
for service in "${services[@]}"; do
    if docker ps | grep -q $service; then
        print_status "$service is running"
    else
        print_error "$service is not running"
    fi
done

# Display access information
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║              Service Access Information                    ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Airflow Web UI:${NC}       http://localhost:8085"
echo -e "                        Username: admin"
echo -e "                        Password: admin"
echo ""
echo -e "${GREEN}Spark Master UI:${NC}      http://localhost:9090"
echo ""
echo -e "${GREEN}Spark History Server:${NC} http://localhost:18080"
echo ""
echo -e "${GREEN}MinIO Console:${NC}        http://localhost:9001"
echo -e "                        Username: admin"
echo -e "                        Password: password"
echo ""
echo -e "${GREEN}Metabase:${NC}             http://localhost:3000"
echo -e "                        (Create account on first visit)"
echo ""
echo -e "${GREEN}Jupyter Notebook:${NC}     http://localhost:8888"
echo ""
echo -e "${GREEN}Dremio:${NC}               http://localhost:9047"
echo ""

# Show next steps
echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    Next Steps                              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo "1. Verify all services are running:"
echo "   docker-compose ps"
echo ""
echo "2. Check Airflow logs:"
echo "   docker-compose logs -f airflow-scheduler"
echo ""
echo "3. Trigger the pipeline from Airflow UI:"
echo "   - Go to http://localhost:8085"
echo "   - Find 'product_sales_bronze_silver_gold_pipeline'"
echo "   - Toggle it ON and click 'Trigger DAG'"
echo ""
echo "4. Monitor pipeline execution:"
echo "   - Airflow UI: http://localhost:8085"
echo "   - Spark Master: http://localhost:9090"
echo ""
echo "5. Set up Metabase dashboard:"
echo "   - Go to http://localhost:3000"
echo "   - Create admin account"
echo "   - Connect to MySQL database"
echo "   - Database: analytics"
echo "   - Host: mysql"
echo "   - Port: 3306"
echo "   - Username: root"
echo "   - Password: rootpassword"
echo ""
echo -e "${GREEN}Setup completed successfully!${NC}"
echo ""