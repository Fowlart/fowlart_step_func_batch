import os
import sys
import boto3
from botocore.exceptions import ClientError
from delta import configure_spark_with_delta_pip
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import phonenumbers

def normalize_phone(phone_raw):
    if not phone_raw:
        return None
    results = []
    for part in phone_raw.split(","):
        part = part.strip()
        try:
            parsed = phonenumbers.parse(part, None)
            if phonenumbers.is_valid_number(parsed):
                results.append(phonenumbers.format_number(
                    parsed, phonenumbers.PhoneNumberFormat.E164
                ))
        except Exception:
            pass
    return results[0] if len(results) == 1 else (results if results else None)


def normalize_linkedin(linkedin_url):
    if not linkedin_url:
        return None
    # Extract slug from full URL: take the path segment after /in/
    match = re.search(r"linkedin\.com/in/([^/]+)", linkedin_url)
    slug = match.group(1) if match else linkedin_url
    # Remove hyphens and lowercase
    slug = re.sub(r"[-\s]", "", slug).lower()
    return slug or None


def normalize_name(full_name, first_name, last_name, source):
    if source == "pirical":
        raw = f"{first_name or ''} {last_name or ''}".strip()
    else:
        raw = full_name or ""
    if not raw:
        return None
    name = raw.lower().strip()
    for prefix in ["dr.", "mr.", "ms.", "mrs.", "prof."]:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
    return name or None


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

    # fetch configurations from SSM instead of environment variables
    try:
        print("--- Fetching configurations from SSM ---", flush=True)

        s3_bucket = get_ssm_parameter('/mdm/s3_bucket', with_decryption=False)
        pg_url = get_ssm_parameter('/mdm/pg_url', with_decryption=True)

        print(f"Successfully resolved S3 Bucket: {s3_bucket}", flush=True)
        print(f"Successfully resolved PG URL: {pg_url}", flush=True)
    except Exception as e:
        print(f"Initialization failed due to SSM error: {e}", flush=True)
        sys.exit(1)

    lawyers_raw_path = s3_bucket + f"er/incoming_lawyers/incoming_lawyers.json"

    print(f"--- Lawyers raw path: {lawyers_raw_path} ---")

    # initialize SparkSession with S3 dependencies
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

    # inject Delta Lake dependencies into the SparkSession
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    try:
        normalize_name_udf = udf(normalize_name, StringType())

        normalize_linkedin_udf = udf(normalize_linkedin, StringType())

        # read incoming_lawyers.json
        lawyers_raw_df = (
            spark
            .read
            .option("multiLine", "true")
            .json(lawyers_raw_path))

        lawyer_df = lawyers_raw_df.withColumn(
            "full_name_normalized",
            normalize_name_udf(
                F.col("full_name"),
                F.col("first_name"),
                F.col("last_name"),
                F.col("source")))

        lawyer_df = (
            lawyer_df
            .withColumn("linkedin_slug",normalize_linkedin_udf(F.col("linkedin_url"), F.col("source"))))


    except Exception as e:
        print(f"Error during S3 read or processing: {e}")
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()