"""
For this code to work, we need python 3.11.8, PySpark 3.5.* Delta Lake 2.4.*
"""

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, explode, lit, to_timestamp, udf
from pyspark.sql.types import StringType

# Configuration
SERVICE_ACCOUNT_KEY = "calm-depot-454710-p0-4cf918c71f69.json"
RAW_JSON_PATH = "gs://group-1-landing-lets-talk/stack_exchange/stackexchange_raw_*.json"
TRUSTED_DELTA_PATH = "gs://group-1-delta-lake-lets-talk/trusted_zone/stack_exchange"

# Stack Exchange site to main topic mapping
TOPIC_MAP = {
    # Tech related
    "stackoverflow": "Tech",
    "softwareengineering": "Tech",
    "superuser": "Tech",
    "askubuntu": "Tech",
    
    # Entertainment/Film related
    "movies": "Film",
    "gaming": "Film",  # Gaming/entertainment
    "worldbuilding": "Film",  # Creative/entertainment
    
    # Other topics (can be expanded based on needs)
    "cooking": "Other",
    "travel": "Other",
    "philosophy": "Other"
}

def spark_configure(app_name: str):
    """Configure Spark with Delta Lake and GCS support."""
    conf = (
        pyspark.conf.SparkConf()
        .setAppName(app_name)
        .setMaster("local[*]")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY)
        .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .set("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
        .set("spark.sql.shuffle.partitions", "4")
    )
    builder = pyspark.sql.SparkSession.builder.config(conf=conf)
    return configure_spark_with_delta_pip(builder).getOrCreate()

if __name__ == "__main__":
    try:
        print(" Starting Stack Exchange data transformation...")
        spark = spark_configure("stackexchange_to_trusted")
        spark.sparkContext.setLogLevel("ERROR")  # Reduce logging noise
        
        print(" Reading raw data from landing zone...")
        df_raw = spark.read.option("multiline", "true").json(RAW_JSON_PATH)
        print(f"✓ Found {df_raw.count()} raw records")
        
        print("\n Transforming data structure...")
        # Explode and standardize the data
        df_exploded = (
            df_raw
            .select(
                "site",
                explode(col("pages")).alias("page")
            )
            .select(
                col("site"),
                explode(col("page.items")).alias("item")
            )
            .select(
                col("site"),
                col("item.question_id").alias("id"),
                col("item.title").alias("title"),
                col("item.body").alias("body"),
                col("item.owner.display_name").alias("author"),
                col("item.score").alias("score"),
                col("item.answer_count").alias("answer_count"),
                col("item.creation_date").alias("created_utc"),
                col("item.tags").alias("tags")
            )
        )
        
        print(" Cleaning and standardizing...")
        # Clean and standardize
        df_clean = (
            df_exploded
            .dropDuplicates(["id"])
            .filter(
                col("id").isNotNull() &
                col("title").isNotNull()
            )
            .withColumn(
                "created_utc_ts",
                to_timestamp(col("created_utc"))
            )
        )
        
        print(" Mapping topics...")
        # Map to main topics and prepare final dataset
        topic_udf = udf(lambda s: TOPIC_MAP.get(s, "Other"), StringType())
        
        df_trusted = (
            df_clean
            .withColumn("source", lit("stack_exchange"))
            .withColumn("topic", topic_udf(col("site")))
            .withColumn("original_topic", col("site"))
            .select(
                "source",
                "original_topic",
                "topic",
                "id",
                "author",
                "title",
                col("tags").alias("keywords"),
                col("score").alias("upvotes"),
                col("answer_count").alias("num_comments"),
                "created_utc_ts",
                "body"
            )
        )
        
        print("\n Writing to trusted zone...")
        # Write to trusted zone with schema overwrite
        (df_trusted.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(TRUSTED_DELTA_PATH))
        
        print(f" Data successfully written to: {TRUSTED_DELTA_PATH}")
        
        # Print some statistics
        total_records = df_trusted.count()
        records_by_topic = df_trusted.groupBy("topic").count().orderBy("topic").collect()
        
        print("\n Final Statistics:")
        print(f"Total records processed: {total_records}")
        print("\nRecords by topic:")
        for row in records_by_topic:
            print(f"  - {row['topic']}: {row['count']}")
        
        print("\n🎉 Transformation completed successfully!")
        spark.stop()
        
    except Exception as e:
        print(f"\n Error during transformation: {str(e)}")
        raise 
