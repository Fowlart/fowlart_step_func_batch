from pyspark.sql import SparkSession
import datetime
import sys


def main():
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

    print(f"--- Starting the script execution at {ts} ---")

    # Правильна логіка зчитування
    # sys.argv[0] - назва скрипта
    # sys.argv[1] - n (кількість партіцій)
    # sys.argv[2] - шлях

    partition_num: int = int(sys.argv[1]) if len(sys.argv) > 0 else 1

    input_path = sys.argv[2] if len(sys.argv) > 2 else "s3a://fowlart-demo-bucket/spark_output/"

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


    # reading multiline JSON
    try:
        df = (spark
              .read
              .parquet(input_path))

        row_count = df.count()
        print(f"--- Row count: {row_count} ---")
        print(f"--- Writing results back to S3: {input_path} ---")

        (df
         .repartition(partition_num)
         .write
         .mode("overwrite")
         .parquet(input_path))

    except Exception as e:
        print(f"Error with S3 connection: {e}")

    spark.stop()


if __name__ == "__main__":
    main()