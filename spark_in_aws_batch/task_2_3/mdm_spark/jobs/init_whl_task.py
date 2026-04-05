import os
import sys
import boto3
from botocore.exceptions import ClientError
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_ssm_parameter(param_name: str, with_decryption: bool = False) -> str:
    """Fetches a parameter from AWS SSM Parameter Store."""
    try:
        # Initialize boto3 client for SSM
        ssm_client = boto3.client('ssm', region_name='us-east-1')
        response = ssm_client.get_parameter(
            Name=param_name,
            WithDecryption=with_decryption # Required for SecureString
        )
        return response['Parameter']['Value']
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Failed to fetch parameter {param_name}. Error: {error_code}", flush=True)
        # Specifically catching AccessDeniedException for the sandbox verification
        if error_code == 'AccessDeniedException':
            print("AccessDeniedException confirmed. Please check IAM role permissions.", flush=True)
        raise e


def main():

    # Just reading from env, to demo
    # Get a required variable (raises KeyError if missing)
    s3_input_path = os.environ['S3_INPUT_PATH']
    print(f"--- Target S3 Path from environment: {s3_input_path} ---")

    # Fetch configurations from SSM instead of environment variables
    try:
        print("--- Fetching configurations from SSM ---", flush=True)
        s3_bucket = get_ssm_parameter('/mdm/s3_bucket', with_decryption=False)
        pg_url = get_ssm_parameter('/mdm/pg_url', with_decryption=True)

        print(f"Successfully resolved S3 Bucket: {s3_bucket}", flush=True)
        print(f"Successfully resolved PG URL: {pg_url}", flush=True)
    except Exception as e:
        print(f"Initialization failed due to SSM error: {e}", flush=True)
        sys.exit(1)


    # Read path from environment variable as specified in Task 2
    s3_input_path = s3_bucket + f"er/clusters/existing_clusters.json"
    print(f"--- Target S3 Path: {s3_input_path} ---")

    # Initialize SparkSession with S3 dependencies
    builder = (SparkSession
             .builder
             .appName("MDM_whl_Task2")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
             .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
             .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
             .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
             .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
             .config("spark.hadoop.fs.s3a.connection.ttl", "86400000"))

    # Inject Delta Lake dependencies into the SparkSession
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    try:
        # Read incoming_lawyers.json
        df = spark.read.option("multiLine", "true").json(s3_input_path)

        # Print schema and record count
        print("--- DataFrame Schema ---")
        df.printSchema()

        row_count = df.count()
        print(f"--- Row count: {row_count} ---")

    except Exception as e:
        print(f"Error during S3 read or processing: {e}")
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()