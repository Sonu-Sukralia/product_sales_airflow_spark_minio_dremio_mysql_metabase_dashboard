"""
Silver Layer: Data validation with Amazon Deequ and transformation
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, upper, current_timestamp
import json
from pyspark.sql.functions import sum as spark_sum
def create_spark_session():
    """Create Spark session with S3 and Deequ configuration"""
    return SparkSession.builder \
        .appName("ValidateTransformSales-Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def run_deequ_checks(spark, df):
    """Run data quality checks using Amazon Deequ"""
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.verification import VerificationSuite, VerificationResult
    
    check = Check(spark, CheckLevel.Warning, "Sales Data Quality Checks")
    
    checkResult = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(
            check.hasSize(lambda x: x > 0, "Dataset should not be empty")
                 .isComplete("product_name")
                 .isComplete("quantity")
                 .isComplete("unit_price")
                 .isComplete("total_amount")
                 .isNonNegative("quantity")
                 .isNonNegative("unit_price")
                 .isNonNegative("total_amount")
                 .isContainedIn("payment_method", ["Credit Card", "Debit Card", "UPI", "Cash", "Net Banking"])
                 .isContainedIn("region", ["North", "South", "East", "West", "Central"])
                 .hasPattern("customer_id", r"^CUST\d{4}$")
                 .hasPattern("store_id", r"^STORE\d{3}$")
        ).run()
    
    checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    
    return checkResult, checkResult_df

def manual_data_quality_checks(df):
    """Manual data quality validation"""
    
    print("\n" + "="*80)
    print("DATA QUALITY CHECKS")
    print("="*80)
    
    total_records = df.count()
    print(f"\n📊 Total Records: {total_records}")
    
    # Check for null values
    print("\n🔍 NULL VALUE ANALYSIS:")
    null_counts = df.select([
        (col(c).isNull().cast("int").alias(c)) for c in df.columns
    ]).agg(*[
        spark_sum(col(c)).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    
    for column, null_count in null_counts.items():
        if null_count > 0:
            percentage = (null_count / total_records) * 100
            print(f"  ❌ {column}: {null_count} nulls ({percentage:.2f}%)")
    
    # Check for negative values
    print("\n🔍 NEGATIVE VALUE CHECKS:")
    negative_quantity = df.filter(col("quantity") < 0).count()
    negative_price = df.filter(col("unit_price") < 0).count()
    negative_amount = df.filter(col("total_amount") < 0).count()
    
    if negative_quantity > 0:
        print(f"  ❌ Negative quantities: {negative_quantity}")
    if negative_price > 0:
        print(f"  ❌ Negative prices: {negative_price}")
    if negative_amount > 0:
        print(f"  ❌ Negative amounts: {negative_amount}")
    
    # Check for empty strings
    print("\n🔍 EMPTY STRING CHECKS:")
    empty_product_name = df.filter((col("product_name") == "") | (col("product_name").isNull())).count()
    if empty_product_name > 0:
        print(f"  ❌ Empty product names: {empty_product_name}")
    
    # Schema validation
    print("\n🔍 SCHEMA VALIDATION:")
    expected_columns = [
        "transaction_id", "product_name", "product_category", "quantity",
        "unit_price", "discount_percent", "total_amount", "customer_id",
        "store_id", "payment_method", "region", "transaction_date",
        "ingestion_timestamp", "source_system"
    ]
    
    actual_columns = df.columns
    missing_columns = set(expected_columns) - set(actual_columns)
    extra_columns = set(actual_columns) - set(expected_columns)
    
    if missing_columns:
        print(f"  ❌ Missing columns: {missing_columns}")
    else:
        print("  ✅ All expected columns present")
    
    if extra_columns:
        print(f"  ⚠️  Extra columns: {extra_columns}")
    
    print("\n" + "="*80)
    
    # Calculate quality score
    issues = negative_quantity + negative_price + negative_amount + empty_product_name
    quality_score = ((total_records - issues) / total_records) * 100
    
    quality_report = {
        "total_records": total_records,
        "quality_score": quality_score,
        "issues": {
            "null_values": null_counts,
            "negative_quantity": negative_quantity,
            "negative_price": negative_price,
            "negative_amount": negative_amount,
            "empty_product_name": empty_product_name
        }
    }
    
    return quality_report

def clean_and_transform(df):
    """Clean and transform data for Silver layer"""
    
    print("\n🧹 CLEANING AND TRANSFORMING DATA...")
    
    # Remove records with critical quality issues
    clean_df = df.filter(
        (col("product_name").isNotNull()) &
        (col("product_name") != "") &
        (col("quantity").isNotNull()) &
        (col("quantity") > 0) &
        (col("unit_price").isNotNull()) &
        (col("unit_price") > 0) &
        (col("total_amount").isNotNull()) &
        (col("total_amount") > 0)
    )
    
    # Standardize text fields
    clean_df = clean_df.withColumn("product_name", trim(upper(col("product_name")))) \
                       .withColumn("payment_method", trim(col("payment_method"))) \
                       .withColumn("region", trim(col("region")))
    
    # Add validation flag
    clean_df = clean_df.withColumn("data_quality_flag", when(
        (col("discount_percent") > 50), "high_discount"
    ).otherwise("valid"))
    
    # Add processing timestamp
    clean_df = clean_df.withColumn("silver_processing_timestamp", current_timestamp())
    
    records_before = df.count()
    records_after = clean_df.count()
    records_removed = records_before - records_after
    
    print(f"  📊 Records before cleaning: {records_before}")
    print(f"  ✅ Records after cleaning: {records_after}")
    print(f"  🗑️  Records removed: {records_removed} ({(records_removed/records_before)*100:.2f}%)")
    
    return clean_df

def save_quality_report(spark, quality_report, output_path):
    """Save quality report to MinIO"""
    
    report_df = spark.createDataFrame([quality_report])
    
    report_df.write \
        .mode("overwrite") \
        .json(output_path)
    
    print(f"\n📄 Quality report saved to: {output_path}")

def main():
    parser = argparse.ArgumentParser(description='Validate and transform sales data')
    parser.add_argument('--date', required=True, help='Date in YYYY-MM-DD format')
    parser.add_argument('--input-path', required=True, help='Input path from bronze layer')
    parser.add_argument('--output-path', required=True, help='Output path for silver layer')
    parser.add_argument('--quality-output', required=True, help='Output path for quality reports')
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    try:
        print(f"\n{'='*80}")
        print(f"SILVER LAYER PROCESSING - {args.date}")
        print(f"{'='*80}\n")
        
        # Read bronze data
        print(f"📖 Reading bronze data from: {args.input_path}")
        df = spark.read.parquet(args.input_path)
        
        print(f"📊 Bronze records loaded: {df.count()}")
        
        # Run quality checks
        quality_report = manual_data_quality_checks(df)
        
        # Clean and transform
        clean_df = clean_and_transform(df)
        
        # Write to Silver layer
        print(f"\n💾 Writing clean data to: {args.output_path}")
        clean_df.write \
            .mode("overwrite") \
            .parquet(args.output_path)
        
        # Save quality report
        save_quality_report(spark, quality_report, args.quality_output)
        
        # Show sample
        print("\n📋 SAMPLE CLEAN DATA:")
        clean_df.select(
            "transaction_id", "product_name", "quantity", 
            "unit_price", "total_amount", "region"
        ).show(10, truncate=False)
        
        print("\n✅ Silver layer processing completed successfully")
        
    except Exception as e:
        print(f"\n❌ Error in validation and transformation: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()