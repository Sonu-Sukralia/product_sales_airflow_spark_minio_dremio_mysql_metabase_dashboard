# """
# Gold Layer: Business aggregations and analytics
# """
# import argparse
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
#     current_timestamp, year, month, dayofmonth, to_date, lit, round as spark_round,
#     row_number, dense_rank, rank
# )
# from pyspark.sql.window import Window
# from datetime import datetime
# from pyspark.sql.functions import when
# def create_spark_session():
#     """Create Spark session with S3 configuration"""
#     return SparkSession.builder \
#         .appName("AggregateSales-Gold") \
#         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#         .config("spark.hadoop.fs.s3a.access.key", "admin") \
#         .config("spark.hadoop.fs.s3a.secret.key", "password") \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
#         .getOrCreate()

# def create_daily_aggregates(df, date_str):
#     """Create daily sales aggregates"""
    
#     print("\n📊 Creating daily aggregates...")
    
#     daily_agg = df.groupBy("transaction_date", "product_name", "product_category", "region") \
#         .agg(
#             spark_sum("quantity").alias("total_quantity_sold"),
#             spark_sum("total_amount").alias("total_revenue"),
#             avg("unit_price").alias("avg_unit_price"),
#             avg("discount_percent").alias("avg_discount_percent"),
#             count("transaction_id").alias("transaction_count"),
#             spark_max("total_amount").alias("max_transaction_amount"),
#             spark_min("total_amount").alias("min_transaction_amount")
#         ) \
#         .withColumn("processing_timestamp", current_timestamp()) \
#         .withColumn("revenue_rounded", spark_round(col("total_revenue"), 2)) \
#         .withColumn("avg_unit_price_rounded", spark_round(col("avg_unit_price"), 2))
    
#     # Add ranking
#     window_spec = Window.partitionBy("transaction_date").orderBy(col("total_revenue").desc())
#     daily_agg = daily_agg.withColumn("revenue_rank", rank().over(window_spec))
    
#     print(f"  ✅ Daily aggregates created: {daily_agg.count()} records")
    
#     return daily_agg

# def create_monthly_aggregates(spark, silver_path):
#     """Create monthly aggregates by reading all available silver data"""
    
#     print("\n📊 Creating monthly aggregates...")
    
#     try:
#         # Read all available silver data
#         all_silver_df = spark.read.parquet(f"{silver_path.rsplit('/', 2)[0]}/*/*")
        
#         # Extract year and month
#         monthly_df = all_silver_df.withColumn("year", year(to_date(col("transaction_date")))) \
#                                    .withColumn("month", month(to_date(col("transaction_date"))))
        
#         # Aggregate by month
#         monthly_agg = monthly_df.groupBy("year", "month", "product_name", "product_category") \
#             .agg(
#                 spark_sum("quantity").alias("total_quantity_sold"),
#                 spark_sum("total_amount").alias("total_revenue"),
#                 avg("unit_price").alias("avg_unit_price"),
#                 count("transaction_id").alias("transaction_count"),
#                 count(col("customer_id")).alias("unique_customers_approx")
#             ) \
#             .withColumn("processing_timestamp", current_timestamp()) \
#             .withColumn("revenue_rounded", spark_round(col("total_revenue"), 2))
        
#         # Add month name for better readability
#         monthly_agg = monthly_agg.withColumn("month_name", 
#             when(col("month") == 1, "January")
#             .when(col("month") == 2, "February")
#             .when(col("month") == 3, "March")
#             .when(col("month") == 4, "April")
#             .when(col("month") == 5, "May")
#             .when(col("month") == 6, "June")
#             .when(col("month") == 7, "July")
#             .when(col("month") == 8, "August")
#             .when(col("month") == 9, "September")
#             .when(col("month") == 10, "October")
#             .when(col("month") == 11, "November")
#             .when(col("month") == 12, "December")
#         )
        
#         # Sort by revenue
#         monthly_agg = monthly_agg.orderBy(col("year").desc(), col("month").desc(), col("total_revenue").desc())
        
#         print(f"  ✅ Monthly aggregates created: {monthly_agg.count()} records")
        
#         return monthly_agg
        
#     except Exception as e:
#         print(f"  ⚠️  Could not create monthly aggregates: {str(e)}")
#         print("  ℹ️  This is normal for the first run - monthly data will accumulate over time")
#         return None

# def create_product_analytics(spark, silver_path):
#     """Create product-level analytics"""
    
#     print("\n📊 Creating product analytics...")
    
#     try:
#         # Read all available silver data
#         all_silver_df = spark.read.parquet(f"{silver_path.rsplit('/', 2)[0]}/*/*")
        
#         # Product analytics
#         product_analytics = all_silver_df.groupBy("product_name", "product_category") \
#             .agg(
#                 spark_sum("total_amount").alias("lifetime_revenue"),
#                 spark_sum("quantity").alias("lifetime_quantity_sold"),
#                 avg("unit_price").alias("avg_selling_price"),
#                 avg("discount_percent").alias("avg_discount"),
#                 count("transaction_id").alias("total_transactions"),
#                 count(col("customer_id")).alias("total_customers_approx")
#             ) \
#             .withColumn("processing_timestamp", current_timestamp()) \
#             .withColumn("lifetime_revenue_rounded", spark_round(col("lifetime_revenue"), 2))
        
#         # Add ranking
#         window_spec = Window.orderBy(col("lifetime_revenue").desc())
#         product_analytics = product_analytics.withColumn("revenue_rank", rank().over(window_spec))
        
#         # Calculate market share
#         total_revenue = product_analytics.select(spark_sum("lifetime_revenue")).collect()[0][0]
#         product_analytics = product_analytics.withColumn(
#             "market_share_percent", 
#             spark_round((col("lifetime_revenue") / total_revenue) * 100, 2)
#         )
        
#         # Sort by revenue
#         product_analytics = product_analytics.orderBy(col("lifetime_revenue").desc())
        
#         print(f"  ✅ Product analytics created: {product_analytics.count()} records")
        
#         # Show top products
#         print("\n🏆 TOP 10 PRODUCTS BY REVENUE:")
#         product_analytics.select(
#             "revenue_rank", "product_name", "lifetime_revenue_rounded", 
#             "lifetime_quantity_sold", "market_share_percent"
#         ).show(10, truncate=False)
        
#         return product_analytics
        
#     except Exception as e:
#         print(f"  ⚠️  Could not create product analytics: {str(e)}")
#         return None

# def create_region_analytics(df):
#     """Create region-wise sales analytics"""
    
#     print("\n📊 Creating region analytics...")
    
#     region_agg = df.groupBy("region", "product_category") \
#         .agg(
#             spark_sum("total_amount").alias("total_revenue"),
#             spark_sum("quantity").alias("total_quantity"),
#             count("transaction_id").alias("transaction_count")
#         ) \
#         .withColumn("processing_timestamp", current_timestamp()) \
#         .withColumn("revenue_rounded", spark_round(col("total_revenue"), 2))
    
#     print(f"  ✅ Region analytics created: {region_agg.count()} records")
    
#     return region_agg

# def main():
#     parser = argparse.ArgumentParser(description='Create gold layer aggregates')
#     parser.add_argument('--date', required=True, help='Date in YYYY-MM-DD format')
#     parser.add_argument('--input-path', required=True, help='Input path from silver layer')
#     parser.add_argument('--output-path-daily', required=True, help='Output path for daily aggregates')
#     parser.add_argument('--output-path-monthly', required=True, help='Output path for monthly aggregates')
#     parser.add_argument('--output-path-product', required=True, help='Output path for product analytics')
    
#     args = parser.parse_args()
    
#     spark = create_spark_session()
    
#     try:
#         print(f"\n{'='*80}")
#         print(f"GOLD LAYER PROCESSING - {args.date}")
#         print(f"{'='*80}\n")
        
#         # Read silver data for the current date
#         print(f"📖 Reading silver data from: {args.input_path}")
#         df = spark.read.parquet(args.input_path)
#         print(f"📊 Silver records loaded: {df.count()}")
        
#         # Create daily aggregates
#         daily_agg = create_daily_aggregates(df, args.date)
#         print(f"\n💾 Writing daily aggregates to: {args.output_path_daily}")
#         daily_agg.write.mode("overwrite").parquet(args.output_path_daily)
        
#         # Create monthly aggregates (reads all historical data)
#         monthly_agg = create_monthly_aggregates(spark, args.input_path)
#         if monthly_agg:
#             print(f"\n💾 Writing monthly aggregates to: {args.output_path_monthly}")
#             monthly_agg.write.mode("overwrite").parquet(args.output_path_monthly)
        
#         # Create product analytics (reads all historical data)
#         product_analytics = create_product_analytics(spark, args.input_path)
#         if product_analytics:
#             print(f"\n💾 Writing product analytics to: {args.output_path_product}")
#             product_analytics.write.mode("overwrite").parquet(args.output_path_product)
        
#         # Create region analytics
#         region_agg = create_region_analytics(df)
        
#         # Show sample daily aggregates
#         print("\n📋 SAMPLE DAILY AGGREGATES:")
#         daily_agg.select(
#             "transaction_date", "product_name", "total_quantity_sold",
#             "revenue_rounded", "transaction_count", "revenue_rank"
#         ).show(10, truncate=False)
        
#         print("\n✅ Gold layer processing completed successfully")
        
#     except Exception as e:
#         print(f"\n❌ Error in aggregation: {str(e)}")
#         raise
#     finally:
#         spark.stop()

# if __name__ == "__main__":
#     main()













































































"""
Gold Layer: Business aggregations and analytics
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    current_timestamp, year, month, dayofmonth, to_date, lit, round as spark_round,
    row_number, dense_rank, rank
)
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.functions import when

def create_spark_session():
    """Create Spark session with S3 configuration"""
    return SparkSession.builder \
        .appName("AggregateSales-Gold") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def create_daily_aggregates(df, date_str):
    """Create daily sales aggregates"""
    
    print("\n📊 Creating daily aggregates...")
    
    daily_agg = df.groupBy("transaction_date", "product_name", "product_category", "region") \
        .agg(
            spark_sum("quantity").alias("total_quantity_sold"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("unit_price"), 2).alias("avg_unit_price"),
            spark_round(avg("discount_percent"), 2).alias("avg_discount_percent"),
            count("transaction_id").alias("transaction_count"),
            spark_round(spark_max("total_amount"), 2).alias("max_transaction_amount"),
            spark_round(spark_min("total_amount"), 2).alias("min_transaction_amount")
        ) \
        .withColumn("processing_timestamp", current_timestamp())
    
    # Add ranking
    window_spec = Window.partitionBy("transaction_date").orderBy(col("total_revenue").desc())
    daily_agg = daily_agg.withColumn("revenue_rank", rank().over(window_spec))
    
    print(f"  ✅ Daily aggregates created: {daily_agg.count()} records")
    
    return daily_agg

def create_monthly_aggregates(spark, silver_path):
    """Create monthly aggregates by reading all available silver data"""
    
    print("\n📊 Creating monthly aggregates...")
    
    try:
        # Read all available silver data
        all_silver_df = spark.read.parquet(f"{silver_path.rsplit('/', 2)[0]}/*/*")
        
        # Extract year and month
        monthly_df = all_silver_df.withColumn("year", year(to_date(col("transaction_date")))) \
                                   .withColumn("month", month(to_date(col("transaction_date"))))
        
        # Aggregate by month
        monthly_agg = monthly_df.groupBy("year", "month", "product_name", "product_category") \
            .agg(
                spark_sum("quantity").alias("total_quantity_sold"),
                spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
                spark_round(avg("unit_price"), 2).alias("avg_unit_price"),
                count("transaction_id").alias("transaction_count"),
                count(col("customer_id")).alias("unique_customers_approx")
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("revenue_rounded", spark_round(col("total_revenue"), 2))
        
        # Add month name for better readability
        monthly_agg = monthly_agg.withColumn("month_name", 
            when(col("month") == 1, "January")
            .when(col("month") == 2, "February")
            .when(col("month") == 3, "March")
            .when(col("month") == 4, "April")
            .when(col("month") == 5, "May")
            .when(col("month") == 6, "June")
            .when(col("month") == 7, "July")
            .when(col("month") == 8, "August")
            .when(col("month") == 9, "September")
            .when(col("month") == 10, "October")
            .when(col("month") == 11, "November")
            .when(col("month") == 12, "December")
        )
        
        # Sort by revenue
        monthly_agg = monthly_agg.orderBy(col("year").desc(), col("month").desc(), col("total_revenue").desc())
        
        print(f"  ✅ Monthly aggregates created: {monthly_agg.count()} records")
        
        return monthly_agg
        
    except Exception as e:
        print(f"  ⚠️  Could not create monthly aggregates: {str(e)}")
        print("  ℹ️  This is normal for the first run - monthly data will accumulate over time")
        return None

def create_product_analytics(spark, silver_path):
    """Create product-level analytics"""
    
    print("\n📊 Creating product analytics...")
    
    try:
        # Read all available silver data
        all_silver_df = spark.read.parquet(f"{silver_path.rsplit('/', 2)[0]}/*/*")
        
        # Product analytics
        product_analytics = all_silver_df.groupBy("product_name", "product_category") \
            .agg(
                spark_round(spark_sum("total_amount"), 2).alias("lifetime_revenue"),
                spark_sum("quantity").alias("lifetime_quantity_sold"),
                spark_round(avg("unit_price"), 2).alias("avg_selling_price"),
                spark_round(avg("discount_percent"), 2).alias("avg_discount"),
                count("transaction_id").alias("total_transactions"),
                count(col("customer_id")).alias("total_customers_approx")
            ) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("lifetime_revenue_rounded", spark_round(col("lifetime_revenue"), 2))
        
        # Add ranking
        window_spec = Window.orderBy(col("lifetime_revenue").desc())
        product_analytics = product_analytics.withColumn("revenue_rank", rank().over(window_spec))
        
        # Calculate market share
        total_revenue = product_analytics.select(spark_sum("lifetime_revenue")).collect()[0][0]
        product_analytics = product_analytics.withColumn(
            "market_share_percent", 
            spark_round((col("lifetime_revenue") / total_revenue) * 100, 2)
        )
        
        # Sort by revenue
        product_analytics = product_analytics.orderBy(col("lifetime_revenue").desc())
        
        print(f"  ✅ Product analytics created: {product_analytics.count()} records")
        
        # Show top products
        print("\n🏆 TOP 10 PRODUCTS BY REVENUE:")
        product_analytics.select(
            "revenue_rank", "product_name", "lifetime_revenue_rounded", 
            "lifetime_quantity_sold", "market_share_percent"
        ).show(10, truncate=False)
        
        return product_analytics
        
    except Exception as e:
        print(f"  ⚠️  Could not create product analytics: {str(e)}")
        return None

def create_region_analytics(df):
    """Create region-wise sales analytics"""
    
    print("\n📊 Creating region analytics...")
    
    region_agg = df.groupBy("region", "product_category") \
        .agg(
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_sum("quantity").alias("total_quantity"),
            count("transaction_id").alias("transaction_count")
        ) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("revenue_rounded", spark_round(col("total_revenue"), 2))
    
    print(f"  ✅ Region analytics created: {region_agg.count()} records")
    
    return region_agg

def main():
    parser = argparse.ArgumentParser(description='Create gold layer aggregates')
    parser.add_argument('--date', required=True, help='Date in YYYY-MM-DD format')
    parser.add_argument('--input-path', required=True, help='Input path from silver layer')
    parser.add_argument('--output-path-daily', required=True, help='Output path for daily aggregates')
    parser.add_argument('--output-path-monthly', required=True, help='Output path for monthly aggregates')
    parser.add_argument('--output-path-product', required=True, help='Output path for product analytics')
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    try:
        print(f"\n{'='*80}")
        print(f"GOLD LAYER PROCESSING - {args.date}")
        print(f"{'='*80}\n")
        
        # Read silver data for the current date
        print(f"📖 Reading silver data from: {args.input_path}")
        df = spark.read.parquet(args.input_path)
        print(f"📊 Silver records loaded: {df.count()}")
        
        # Create daily aggregates
        daily_agg = create_daily_aggregates(df, args.date)
        print(f"\n💾 Writing daily aggregates to: {args.output_path_daily}")
        daily_agg.write.mode("overwrite").parquet(args.output_path_daily)
        
        # Create monthly aggregates (reads all historical data)
        monthly_agg = create_monthly_aggregates(spark, args.input_path)
        if monthly_agg:
            print(f"\n💾 Writing monthly aggregates to: {args.output_path_monthly}")
            monthly_agg.write.mode("overwrite").parquet(args.output_path_monthly)
        
        # Create product analytics (reads all historical data)
        product_analytics = create_product_analytics(spark, args.input_path)
        if product_analytics:
            print(f"\n💾 Writing product analytics to: {args.output_path_product}")
            product_analytics.write.mode("overwrite").parquet(args.output_path_product)
        
        # Create region analytics
        region_agg = create_region_analytics(df)
        
        # Show sample daily aggregates
        print("\n📋 SAMPLE DAILY AGGREGATES:")
        daily_agg.select(
            "transaction_date", "product_name", "total_quantity_sold",
            "total_revenue", "transaction_count", "revenue_rank"
        ).show(10, truncate=False)
        
        print("\n✅ Gold layer processing completed successfully")
        
    except Exception as e:
        print(f"\n❌ Error in aggregation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()