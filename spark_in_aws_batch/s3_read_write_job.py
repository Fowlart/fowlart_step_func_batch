from pyspark.sql import SparkSession
import datetime
import sys


def main():
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    print(f"--- Starting the script execution at {ts} ---")

    input_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://fowlart-demo-bucket/raw_data/"

    output_path = sys.argv[2] if len(sys.argv) > 2 else "s3a://fowlart-demo-bucket/spark_output/"

    print(f"--- Reading data from {input_path} ---")

    spark = (SparkSession.builder
             .appName("S3ReadWriteJob")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
              # Get rid of `java.lang.NumberFormatException: For input string: "60s"`
             .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
             .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
             .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
             .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
             .config("spark.hadoop.fs.s3a.connection.ttl", "86400000")
             .getOrCreate())

    print(f"--- Reading data from {input_path} ---")

    # reading multiline JSON
    try:
        df = (spark
              .read
              .option("multiLine", "true")
              .json(input_path))

        df.show()
        row_count = df.count()
        print(f"--- Row count: {row_count} ---")
        print(f"--- Writing results back to S3: {output_path} ---")

        (df
         .write
         .mode("overwrite")
         .parquet(output_path))

    except Exception as e:
        print(f"Error with S3 connection: {e}")

    spark.stop()


if __name__ == "__main__":
    main()