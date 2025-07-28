import pyspark
from delta import configure_spark_with_delta_pip

# Configuration
SERVICE_ACCOUNT_KEY = "calm-depot-454710-p0-4cf918c71f69.json"
TRUSTED_DELTA_PATH = "gs://group-1-delta-lake-lets-talk/trusted_zone/stack_exchange"

def spark_configure():
    conf = (
        pyspark.conf.SparkConf()
        .setAppName("verify_schema")
        .setMaster("local[*]")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY)
        .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .set("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
    )
    builder = pyspark.sql.SparkSession.builder.config(conf=conf)
    return configure_spark_with_delta_pip(builder).getOrCreate()

if __name__ == "__main__":
    print("🔍 Verifying Stack Exchange trusted zone data...")
    spark = spark_configure()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Read the Delta table
    df = spark.read.format("delta").load(TRUSTED_DELTA_PATH)
    
    print("\n Schema:")
    print("=========")
    df.printSchema()
    
    print("\n Sample Records:")
    print("===============")
    df.select("source", "topic", "original_topic", "title", "keywords").show(3, truncate=False)
    
    print("\n Records per Topic:")
    print("==================")
    df.groupBy("topic", "original_topic") \
      .count() \
      .orderBy("topic", "count", ascending=[True, False]) \
      .show(truncate=False)
    
    # Check if we have keywords (previously tags)
    print("\n Sample Keywords:")
    print("================")
    df.select("title", "keywords") \
      .where("size(keywords) > 0") \
      .show(3, truncate=False)
    
    spark.stop() 