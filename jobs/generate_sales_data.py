"""
Bronze Layer: Generate daily product sales data
"""
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, rand, when, round as spark_round,
    monotonically_increasing_id, expr
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import random

def create_spark_session():
    """Create Spark session with S3 configuration"""
    return SparkSession.builder \
        .appName("GenerateSalesData-Bronze") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def generate_sales_data(spark, date_str, output_path):
    """Generate realistic product sales data"""
    
    # Product catalog with realistic pricing
    products = [
        ("laptop", 45000, 85000),
        ("mobile", 8000, 55000),
        ("mouse", 200, 2500),
        ("tv", 15000, 95000),
        ("hardisk", 2500, 12000),
        ("ram", 1500, 8000),
        ("speaker", 800, 15000),
        ("watch", 1000, 35000),
        ("keyboard", 500, 5000),
        ("mic", 500, 8000),
        ("camera", 15000, 125000)
    ]
    
    # Generate data for each product
    sales_data = []
    
    for product_name, min_price, max_price in products:
        # Random number of transactions per product (50-500 per day)
        num_transactions = random.randint(50, 500)
        
        for _ in range(num_transactions):
            # Generate realistic transaction
            base_price = random.uniform(min_price, max_price)
            quantity = random.randint(1, 5)
            discount = random.uniform(0, 0.25)  # 0-25% discount
            
            price_after_discount = base_price * (1 - discount)
            total_amount = price_after_discount * quantity
            
            # Add some data quality issues for validation (5% bad data)
            if random.random() < 0.05:
                # Introduce data quality issues
                if random.random() < 0.3:
                    quantity = -1  # Negative quantity
                elif random.random() < 0.3:
                    total_amount = None  # Missing amount
                elif random.random() < 0.4:
                    product_name = ""  # Empty product name
            
            sales_data.append({
                "product_name": product_name,
                "product_category": get_category(product_name),
                "quantity": quantity,
                "unit_price": round(base_price, 2),
                "discount_percent": round(discount * 100, 2),
                "total_amount": round(total_amount, 2) if total_amount else None,
                "customer_id": f"CUST{random.randint(1000, 9999)}",
                "store_id": f"STORE{random.randint(1, 50):03d}",
                "payment_method": random.choice(["Credit Card", "Debit Card", "UPI", "Cash", "Net Banking"]),
                "region": random.choice(["North", "South", "East", "West", "Central"])
            })
    
    # Create DataFrame
    schema = StructType([
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_percent", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("region", StringType(), True)
    ])
    
    df = spark.createDataFrame(sales_data, schema)
    
    # Add metadata columns
    df = df.withColumn("transaction_id", monotonically_increasing_id()) \
           .withColumn("transaction_date", lit(date_str)) \
           .withColumn("ingestion_timestamp", current_timestamp()) \
           .withColumn("source_system", lit("POS_SYSTEM"))
    
    # Write to Bronze layer (raw data with potential quality issues)
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"Generated {df.count()} sales transactions for {date_str}")
    print(f"Data written to: {output_path}")
    
    # Show sample
    df.show(10, truncate=False)
    
    return df

def get_category(product_name):
    """Map product to category"""
    categories = {
        "laptop": "Computing",
        "mobile": "Mobile & Accessories",
        "mouse": "Computing",
        "tv": "Electronics",
        "hardisk": "Storage",
        "ram": "Computing",
        "speaker": "Audio",
        "watch": "Wearables",
        "keyboard": "Computing",
        "mic": "Audio",
        "camera": "Photography"
    }
    return categories.get(product_name, "General")

def main():
    parser = argparse.ArgumentParser(description='Generate daily product sales data')
    parser.add_argument('--date', required=True, help='Date in YYYY-MM-DD format')
    parser.add_argument('--output-path', required=True, help='Output path for bronze data')
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    try:
        generate_sales_data(spark, args.date, args.output_path)
        print("✅ Bronze layer data generation completed successfully")
    except Exception as e:
        print(f"❌ Error in data generation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()