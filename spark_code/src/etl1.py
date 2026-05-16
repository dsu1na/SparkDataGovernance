from pyspark.sql import SparkSession
   
def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", "/SparkDataGovernance/jars/*") \
        .config("spark.executor.extraClassPath", "/SparkDataGovernance/jars/*") \
        .config('spark.extraListeners', 'io.openlineage.spark.agent.OpenLineageSparkListener') \
        .config('spark.openlineage.transport.url', 'http://marquez:5000') \
        .config('spark.openlineage.transport.type', 'http') \
        .config('spark.openlineage.namespace', 'ny_data') \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minioserver:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .appName("etl_job_subr").getOrCreate()
    
    df = spark.read.format("parquet").load("s3a://nyctaxi/year=2025/yellow_tripdata_2025-02.parquet")
    df \
        .groupBy("VendorID") \
        .count().alias("trip_count") \
        .write.format("parquet") \
        .mode("overwrite") \
        .save("s3a://destinationbucket/etl_output/")

if __name__ == "__main__":
    main()