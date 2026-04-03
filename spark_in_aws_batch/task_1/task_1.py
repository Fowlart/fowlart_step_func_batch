from pyspark.sql import SparkSession
import datetime
import sys

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

def main():
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    print(f"--- Starting the script execution at {ts} ---")

    input_path: str = sys.argv[1]
    output_delta_path: str = sys.argv[2]

    if not input_path or not output_delta_path:
        raise Exception("Please provide input and output path")

    print(f"--- Reading data from {input_path} ---")

    # Configure Spark builder with necessary Hadoop and Delta configurations
    builder = (SparkSession.builder
             .appName("MDM_Delta_Task1")
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

    raw_df: DataFrame = None

    try:
        raw_df = (spark
              .read
              .option("multiLine", "true")
              .json(input_path))
    except Exception as e:
        print(f"Error with S3 connection or data reading: {e}")
        # Terminate execution gracefully if data cannot be read
        sys.exit(1)

    searching_keys_schema: StructType = StructType([
        StructField("emails", ArrayType(StringType())),
        StructField("linkedin_slugs", ArrayType(StringType())),
        StructField("phones", ArrayType(StringType())),
        StructField("full_names_normalized", ArrayType(StringType()))
    ])

    exploded_emails_df = (
        raw_df
        .withColumn("parsed_keys", F.from_json(F.col("searching_keys"), searching_keys_schema))
        .select("master_id", F.explode(F.col("parsed_keys.emails")).alias("email"))
    )

    # Initial write to Delta table
    (exploded_emails_df
     .write
     .format("delta")
     .mode("overwrite")
     .save(output_delta_path))

    # Apply Z-orderBy for optimized Data Skipping
    delta_table = DeltaTable.forPath(spark, output_delta_path)
    delta_table.optimize().executeZOrderBy("email")

    spark.stop()

if __name__ == "__main__":
    main()