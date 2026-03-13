from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import sys
import subprocess
import os
from datetime import date


def create_spark_session(app_name="SampleETL"):
    """Create and configure SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .getOrCreate()


def extract(spark, input_path):
    """Read JSON data from landing zone."""
    print(f"Reading from: {input_path}")
    df = spark.read.json(input_path)
    print(f"Extracted {df.count()} records")
    return df


def transform(df, run_date):
    """Apply transformations to the data."""
    # Add processing metadata
    df_transformed = df \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("pipeline", lit("airflow_spark_demo")) \
        .withColumn("run_date", lit(run_date))
    
    print("Transformation complete")
    return df_transformed


def clean_output_path(output_path):
    """Clean output path before writing.
    
    This handles a Docker volume quirk where Spark's _temporary directories
    from previous runs can block overwrites due to permission mismatches
    between containers. In production with cloud storage (S3/GCS/ADLS), 
    this is typically not needed.
    
    Uses shell command with force flag to handle cross-container permission issues.
    
    Args:
        output_path: Full path to the date-partitioned output directory
    """
    if os.path.exists(output_path):
        print(f"Cleaning existing output path: {output_path}")
        try:
            # Use shell rm -rf to handle permission issues across containers
            result = subprocess.run(
                ["rm", "-rf", output_path],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                print(f"Warning: cleanup returned non-zero: {result.stderr}")
        except Exception as e:
            # Log but don't fail - Spark may still be able to overwrite
            print(f"Warning: Could not clean output path: {e}")


def load(df, output_path):
    """Write transformed data to gold zone as CSV.
    
    Uses date-partitioned output path to enable:
    - Historical data preservation
    - Backfill capabilities  
    - No file locking issues between different dates
    """
    # Clean output path first (handles Docker volume _temporary issue)
    clean_output_path(output_path)
    
    print(f"Writing to: {output_path}")
    df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"Loaded {df.count()} records to gold zone")


def main(input_path, output_base_path, run_date):
    """Main ETL pipeline.
    
    Args:
        input_path: Path to source JSON files
        output_base_path: Base path for gold zone (e.g., /opt/spark-data/gold)
        run_date: Execution date in YYYY-MM-DD format (from Airflow)
    """
    spark = create_spark_session()
    
    # Construct date-partitioned output path
    # This is the production pattern: /gold/date=2025-12-30/
    output_path = f"{output_base_path}/date={run_date}"
    
    try:
        # ETL Pipeline
        df_raw = extract(spark, input_path)
        df_transformed = transform(df_raw, run_date)
        load(df_transformed, output_path)
        
        print("ETL job completed successfully!")
        
    except Exception as e:
        print(f"ETL job failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    # Parse command line arguments
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/spark-data/landing"
    output_base_path = sys.argv[2] if len(sys.argv) > 2 else "/opt/spark-data/gold"
    run_date = sys.argv[3] if len(sys.argv) > 3 else date.today().isoformat()
    
    main(input_path, output_base_path, run_date)