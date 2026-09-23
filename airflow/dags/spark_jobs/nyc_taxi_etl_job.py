"""
NYC Taxi ETL - PySpark job
Reads from S3, transforms, writes aggregated results back to S3.
Uses JVM AWS SDK for output (avoids Hadoop rename issues with SeaweedFS).
"""
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg, round as _round, hour, dayofweek

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--run-id", required=True)
    return parser.parse_args()

def upload_to_s3_via_jvm(spark, df, output_uri):
    """Collect DataFrame to driver and upload as CSV to S3 via JVM SDK."""
    import csv
    import io
    from urllib.parse import urlparse

    # Parse output URI
    parsed = urlparse(output_uri.replace("s3a://", "s3://", 1))
    bucket = parsed.netloc
    key_prefix = parsed.path.lstrip("/")

    # Get credentials from environment
    endpoint = os.getenv("S3_ENDPOINT", "")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    region = os.getenv("AWS_REGION", "us-east-1")

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("Missing S3 credentials or endpoint")

    # Collect to driver as CSV
    rows = df.collect()
    columns = df.columns
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row[c] for c in columns])
    csv_content = buf.getvalue()
    print(f"Collected {len(rows)} rows, CSV size: {len(csv_content)} bytes")

    # Upload using the JVM's AWS SDK v2. This Spark/Hadoop build ships
    # software.amazon.awssdk.* (bundled by hadoop-aws), not the legacy
    # com.amazonaws.* SDK v1 -- v1 classes resolve to an empty JavaPackage
    # and raise "TypeError: 'JavaPackage' object is not callable".
    jvm = spark.sparkContext._jvm
    credentials = jvm.software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(
        access_key, secret_key
    )
    credentials_provider = jvm.software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
        credentials
    )
    s3_client = (
        jvm.software.amazon.awssdk.services.s3.S3Client.builder()
        .credentialsProvider(credentials_provider)
        .endpointOverride(jvm.java.net.URI.create(endpoint))
        .region(jvm.software.amazon.awssdk.regions.Region.of(region))
        .forcePathStyle(True)
        .build()
    )

    def put_object(key, body):
        request = (
            jvm.software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build()
        )
        s3_client.putObject(request, jvm.software.amazon.awssdk.core.sync.RequestBody.fromString(body))

    s3_key = f"{key_prefix}/nyc_taxi_aggregated.csv"
    print(f"Uploading -> s3://{bucket}/{s3_key}")
    put_object(s3_key, csv_content)

    # Upload _SUCCESS marker
    put_object(f"{key_prefix}/_SUCCESS", "")
    print(f"Output uploaded to s3://{bucket}/{key_prefix}/")

def main():
    args = parse_args()

    print("=" * 70)
    print("NYC Taxi ETL Job")
    print("=" * 70)
    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print(f"Run:    {args.run_id}")
    print("=" * 70)

    spark = SparkSession.builder \
        .appName(f"NYC-Taxi-ETL-{args.run_id}") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read
    print("\nReading data...")
    df = spark.read.parquet(args.input)
    total_rows = df.count()
    print(f"Rows read: {total_rows:,}")

    # Schema
    print("\nSchema:")
    df.printSchema()

    # Clean
    print("\nCleaning...")
    df_clean = df.filter(
        (col("fare_amount") > 0) &
        (col("trip_distance") > 0) &
        (col("passenger_count") > 0)
    )
    clean_rows = df_clean.count()
    print(f"Rows after cleaning: {clean_rows:,}")
    print(f"Rows removed: {total_rows - clean_rows:,}")

    # Transform
    print("\nTransforming...")
    df_transformed = df_clean \
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime")))

    # Aggregate by hour + day of week
    print("\nAggregating...")
    df_agg = df_transformed.groupBy("pickup_hour", "pickup_dayofweek").agg(
        count("*").alias("total_trips"),
        _round(avg("fare_amount"), 2).alias("avg_fare"),
        _round(avg("trip_distance"), 2).alias("avg_distance"),
        _round(_sum("fare_amount"), 2).alias("total_revenue")
    ).orderBy("pickup_hour")

    # Show summary
    print("\nResults summary:")
    df_agg.show(24, truncate=False)

    # Write output using JVM S3 client (avoids SeaweedFS rename issues)
    print(f"\nWriting results to: {args.output}")
    upload_to_s3_via_jvm(spark, df_agg, args.output)

    print("\n" + "=" * 70)
    print("ETL completed successfully!")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    main()