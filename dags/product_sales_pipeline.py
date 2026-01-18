# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.operators.bash import BashOperator
# import logging

# logger = logging.getLogger(__name__)

# default_args = {
#     'owner': 'data-engineer',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG(
#     'product_sales_bronze_silver_gold_pipeline',
#     default_args=default_args,
#     description='Product sales data pipeline with Bronze-Silver-Gold layers and Deequ quality checks',
#     schedule_interval='@daily',
#     catchup=False,
#     tags=['sales', 'product', 'medallion', 'deequ'],
# )

# # Task 1: Generate daily product sales data (Bronze Layer)
# generate_sales_data = SparkSubmitOperator(
#     task_id='generate_sales_data_bronze',
#     application='/opt/airflow/jobs/generate_sales_data.py',
#     conn_id='spark_default',
#     conf={
#         'spark.master': 'spark://spark-master:7077',
#         'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,com.amazon.deequ:deequ:2.0.3-spark-3.3',
#     },
#     application_args=[
#         '--date', '{{ ds }}',
#         '--output-path', 's3a://bronze/sales/daily/date={{ ds }}',
#     ],
#     dag=dag,
# )

# # Task 2: Validate and clean data with Deequ (Silver Layer)
# validate_and_clean_data = SparkSubmitOperator(
#     task_id='validate_clean_data_silver',
#     application='/opt/airflow/jobs/validate_and_transform_sales.py',
#     conn_id='spark_default',
#     conf={
#         'spark.master': 'spark://spark-master:7077',
#         'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,com.amazon.deequ:deequ:2.0.3-spark-3.3',
#     },
#     application_args=[
#         '--date', '{{ ds }}',
#         '--input-path', 's3a://bronze/sales/daily/date={{ ds }}',
#         '--output-path', 's3a://silver/sales/daily/date={{ ds }}',
#         '--quality-output', 's3a://quality-reports/sales/date={{ ds }}',
#     ],
#     dag=dag,
# )

# # Task 3: Aggregate and build analytics (Gold Layer)
# aggregate_sales_analytics = SparkSubmitOperator(
#     task_id='aggregate_sales_analytics_gold',
#     application='/opt/airflow/jobs/aggregate_sales_gold.py',
#     conn_id='spark_default',
#     conf={
#         'spark.master': 'spark://spark-master:7077',
#         'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2',
#     },
#     application_args=[
#         '--date', '{{ ds }}',
#         '--input-path', 's3a://silver/sales/daily/date={{ ds }}',
#         '--output-path-daily', 's3a://gold/sales/daily_aggregates/date={{ ds }}',
#         '--output-path-monthly', 's3a://gold/sales/monthly_aggregates',
#         '--output-path-product', 's3a://gold/sales/product_analytics',
#     ],
#     dag=dag,
# )

# # Task 4: Load Gold data to MySQL for Metabase
# load_to_mysql = SparkSubmitOperator(
#     task_id='load_gold_to_mysql',
#     application='/opt/airflow/jobs/load_to_mysql.py',
#     conn_id='spark_default',
#     conf={
#         'spark.master': 'spark://spark-master:7077',
#         'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,mysql:mysql-connector-java:8.0.33',
#     },
#     application_args=[
#         '--date', '{{ ds }}',
#         '--gold-daily-path', 's3a://gold/sales/daily_aggregates/date={{ ds }}',
#         '--gold-monthly-path', 's3a://gold/sales/monthly_aggregates',
#         '--gold-product-path', 's3a://gold/sales/product_analytics',
#         '--mysql-host', 'mysql',
#         '--mysql-port', '3306',
#         '--mysql-database', 'analytics',
#         '--mysql-user', 'root',
#         '--mysql-password', 'rootpassword',
#     ],
#     dag=dag,
# )

# # Task 5: Data Quality Report
# generate_quality_report = BashOperator(
#     task_id='generate_quality_report',
#     bash_command='echo "Data Quality Report for {{ ds }} completed. Check MinIO bucket: quality-reports/sales/date={{ ds }}"',
#     dag=dag,
# )

# # Define task dependencies
# generate_sales_data >> validate_and_clean_data >> aggregate_sales_analytics >> load_to_mysql >> generate_quality_report


















from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'product_sales_bronze_silver_gold_pipeline',
    default_args=default_args,
    description='Product sales data pipeline with Bronze-Silver-Gold layers and Deequ quality checks',
    schedule_interval='@daily',
    catchup=False,
    tags=['sales', 'product', 'medallion', 'deequ'],
)

# Task 1: Generate daily product sales data (Bronze Layer)
generate_sales_data = SparkSubmitOperator(
    task_id='generate_sales_data_bronze',
    application='/opt/airflow/jobs/generate_sales_data.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,com.amazon.deequ:deequ:2.0.3-spark-3.3',
    },
    application_args=[
        '--date', '{{ ds }}',
        '--output-path', 's3a://bronze/sales/daily/date={{ ds }}',
    ],
    dag=dag,
)

# Task 2: Validate and clean data with Deequ (Silver Layer)
validate_and_clean_data = SparkSubmitOperator(
    task_id='validate_clean_data_silver',
    application='/opt/airflow/jobs/validate_and_transform_sales.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,com.amazon.deequ:deequ:2.0.3-spark-3.3',
    },
    application_args=[
        '--date', '{{ ds }}',
        '--input-path', 's3a://bronze/sales/daily/date={{ ds }}',
        '--output-path', 's3a://silver/sales/daily/date={{ ds }}',
        '--quality-output', 's3a://quality-reports/sales/date={{ ds }}',
    ],
    dag=dag,
)

# Task 3: Aggregate and build analytics (Gold Layer)
aggregate_sales_analytics = SparkSubmitOperator(
    task_id='aggregate_sales_analytics_gold',
    application='/opt/airflow/jobs/aggregate_sales_gold.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2',
    },
    application_args=[
        '--date', '{{ ds }}',
        '--input-path', 's3a://silver/sales/daily/date={{ ds }}',
        '--output-path-daily', 's3a://gold/sales/daily_aggregates/date={{ ds }}',
        '--output-path-monthly', 's3a://gold/sales/monthly_aggregates',
        '--output-path-product', 's3a://gold/sales/product_analytics',
    ],
    dag=dag,
)

# Task 4: Load Gold data to MySQL for Metabase
load_to_mysql = SparkSubmitOperator(
    task_id='load_gold_to_mysql',
    application='/opt/airflow/jobs/load_to_mysql.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,mysql:mysql-connector-java:8.0.33',
    },
    application_args=[
        '--date', '{{ ds }}',
        '--gold-daily-path', 's3a://gold/sales/daily_aggregates/date={{ ds }}',
        '--gold-monthly-path', 's3a://gold/sales/monthly_aggregates',
        '--gold-product-path', 's3a://gold/sales/product_analytics',
        '--mysql-host', 'mysql',
        '--mysql-port', '3306',
        '--mysql-database', 'analytics',
        '--mysql-user', 'root',
        '--mysql-password', 'rootpassword',
    ],
    dag=dag,
)

# Task 5: Data Quality Report
generate_quality_report = BashOperator(
    task_id='generate_quality_report',
    bash_command='echo "Data Quality Report for {{ ds }} completed. Check MinIO bucket: quality-reports/sales/date={{ ds }}"',
    dag=dag,
)

# Define task dependencies
generate_sales_data >> validate_and_clean_data >> aggregate_sales_analytics >> load_to_mysql >> generate_quality_report