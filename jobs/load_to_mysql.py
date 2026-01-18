# """
# Load Gold layer data to MySQL for Metabase visualization
# """
# import argparse
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit

# def create_spark_session():
#     """Create Spark session with S3 and MySQL configuration"""
#     return SparkSession.builder \
#         .appName("LoadToMySQL-Gold") \
#         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#         .config("spark.hadoop.fs.s3a.access.key", "admin") \
#         .config("spark.hadoop.fs.s3a.secret.key", "password") \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
#         .getOrCreate()

# def load_to_mysql(df, table_name, jdbc_url, properties, mode="append"):
#     """Load DataFrame to MySQL"""
    
#     print(f"\n💾 Loading data to MySQL table: {table_name}")
#     print(f"   Mode: {mode}")
#     print(f"   Records: {df.count()}")
    
#     df.write \
#         .jdbc(
#             url=jdbc_url,
#             table=table_name,
#             mode=mode,
#             properties=properties
#         )
    
#     print(f"   ✅ Data loaded successfully to {table_name}")

# def main():
#     parser = argparse.ArgumentParser(description='Load gold data to MySQL')
#     parser.add_argument('--date', required=True, help='Date in YYYY-MM-DD format')
#     parser.add_argument('--gold-daily-path', required=True, help='Gold daily aggregates path')
#     parser.add_argument('--gold-monthly-path', required=True, help='Gold monthly aggregates path')
#     parser.add_argument('--gold-product-path', required=True, help='Gold product analytics path')
#     parser.add_argument('--mysql-host', required=True, help='MySQL host')
#     parser.add_argument('--mysql-port', required=True, help='MySQL port')
#     parser.add_argument('--mysql-database', required=True, help='MySQL database')
#     parser.add_argument('--mysql-user', required=True, help='MySQL user')
#     parser.add_argument('--mysql-password', required=True, help='MySQL password')
    
#     args = parser.parse_args()
    
#     spark = create_spark_session()
    
#     # JDBC connection properties
#     jdbc_url = f"jdbc:mysql://{args.mysql_host}:{args.mysql_port}/{args.mysql_database}?createDatabaseIfNotExist=true"
#     properties = {
#         "user": args.mysql_user,
#         "password": args.mysql_password,
#         "driver": "com.mysql.cj.jdbc.Driver"
#     }
    
#     try:
#         print(f"\n{'='*80}")
#         print(f"LOADING GOLD DATA TO MYSQL - {args.date}")
#         print(f"{'='*80}\n")
#         print(f"🔗 MySQL Connection: {args.mysql_host}:{args.mysql_port}/{args.mysql_database}")
        
#         # Load daily aggregates
#         try:
#             print("\n📊 Processing daily aggregates...")
#             daily_df = spark.read.parquet(args.gold_daily_path)
            
#             # Add date column for partitioning if needed
#             daily_df = daily_df.withColumn("report_date", lit(args.date))
            
#             load_to_mysql(daily_df, "sales_daily_aggregates", jdbc_url, properties, mode="append")
#         except Exception as e:
#             print(f"⚠️  Error loading daily aggregates: {str(e)}")
        
#         # Load monthly aggregates
#         try:
#             print("\n📊 Processing monthly aggregates...")
#             monthly_df = spark.read.parquet(args.gold_monthly_path)
#             load_to_mysql(monthly_df, "sales_monthly_aggregates", jdbc_url, properties, mode="overwrite")
#         except Exception as e:
#             print(f"⚠️  Error loading monthly aggregates: {str(e)}")
#             print("   ℹ️  This is normal for the first run")
        
#         # Load product analytics
#         try:
#             print("\n📊 Processing product analytics...")
#             product_df = spark.read.parquet(args.gold_product_path)
#             load_to_mysql(product_df, "sales_product_analytics", jdbc_url, properties, mode="overwrite")
#         except Exception as e:
#             print(f"⚠️  Error loading product analytics: {str(e)}")
#             print("   ℹ️  This is normal for the first run")
        
#         print("\n✅ MySQL data load completed successfully")
#         print("\n📊 You can now connect Metabase to MySQL and create dashboards!")
#         print(f"   Database: {args.mysql_database}")
#         print(f"   Tables: sales_daily_aggregates, sales_monthly_aggregates, sales_product_analytics")
        
#     except Exception as e:
#         print(f"\n❌ Error loading to MySQL: {str(e)}")
#         raise
#     finally:
#         spark.stop()

# if __name__ == "__main__":
#     main()


"""
Load Gold layer data to MySQL for Metabase visualization
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def create_spark_session():
    """Create Spark session with S3 and MySQL configuration"""
    return SparkSession.builder \
        .appName("LoadToMySQL-Gold") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def load_to_mysql(df, table_name, jdbc_url, properties, mode="append"):
    """Load DataFrame to MySQL"""
    
    print(f"\n💾 Loading data to MySQL table: {table_name}")
    print(f"   Mode: {mode}")
    print(f"   Records: {df.count()}")
    
    df.write \
        .jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=properties
        )
    
    print(f"   ✅ Data loaded successfully to {table_name}")

def main():
    parser = argparse.ArgumentParser(description='Load gold data to MySQL')
    parser.add_argument('--date', required=True, help='Date in YYYY-MM-DD format')
    parser.add_argument('--gold-daily-path', required=True, help='Gold daily aggregates path')
    parser.add_argument('--gold-monthly-path', required=True, help='Gold monthly aggregates path')
    parser.add_argument('--gold-product-path', required=True, help='Gold product analytics path')
    parser.add_argument('--mysql-host', required=True, help='MySQL host')
    parser.add_argument('--mysql-port', required=True, help='MySQL port')
    parser.add_argument('--mysql-database', required=True, help='MySQL database')
    parser.add_argument('--mysql-user', required=True, help='MySQL user')
    parser.add_argument('--mysql-password', required=True, help='MySQL password')
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    # JDBC connection properties
    jdbc_url = f"jdbc:mysql://{args.mysql_host}:{args.mysql_port}/{args.mysql_database}?createDatabaseIfNotExist=true"
    properties = {
        "user": args.mysql_user,
        "password": args.mysql_password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    
    try:
        print(f"\n{'='*80}")
        print(f"LOADING GOLD DATA TO MYSQL - {args.date}")
        print(f"{'='*80}\n")
        print(f"🔗 MySQL Connection: {args.mysql_host}:{args.mysql_port}/{args.mysql_database}")
        
        # Load daily aggregates
        try:
            print("\n📊 Processing daily aggregates...")
            daily_df = spark.read.parquet(args.gold_daily_path)
            
            # Add date column for partitioning if needed
            daily_df = daily_df.withColumn("report_date", lit(args.date))
            
            load_to_mysql(daily_df, "sales_daily_aggregates", jdbc_url, properties, mode="append")
        except Exception as e:
            print(f"⚠️  Error loading daily aggregates: {str(e)}")
        
        # Load monthly aggregates
        try:
            print("\n📊 Processing monthly aggregates...")
            monthly_df = spark.read.parquet(args.gold_monthly_path)
            load_to_mysql(monthly_df, "sales_monthly_aggregates", jdbc_url, properties, mode="overwrite")
        except Exception as e:
            print(f"⚠️  Error loading monthly aggregates: {str(e)}")
            print("   ℹ️  This is normal for the first run")
        
        # Load product analytics
        try:
            print("\n📊 Processing product analytics...")
            product_df = spark.read.parquet(args.gold_product_path)
            load_to_mysql(product_df, "sales_product_analytics", jdbc_url, properties, mode="overwrite")
        except Exception as e:
            print(f"⚠️  Error loading product analytics: {str(e)}")
            print("   ℹ️  This is normal for the first run")
        
        print("\n✅ MySQL data load completed successfully")
        print("\n📊 You can now connect Metabase to MySQL and create dashboards!")
        print(f"   Database: {args.mysql_database}")
        print(f"   Tables: sales_daily_aggregates, sales_monthly_aggregates, sales_product_analytics")
        
    except Exception as e:
        print(f"\n❌ Error loading to MySQL: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()