import re, os
import pyspark
from pyspark.sql.functions import col, lower, regexp_replace, udf, expr
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, lit, explode, hash
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, TimestampType
from summa import keywords
import os

# To avoid python3 executable error
os.environ["PYSPARK_PYTHON"] = "python"

USER = 'stef4'
BUCKET_PATH = "gs://group-1-landing-lets-talk/wikipedia/"


def spark_configure(user, name_operation):
    conf = (
        pyspark.conf.SparkConf()
        .setAppName(name_operation)
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .set("spark.files.cleanup", "false")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "C:\\Users\\" + user + "\\.gcs\\calm-depot-454710-p0-4cf918c71f69.json")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .set(
            "spark.jars",
            "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar",
        )
        .setMaster("local[*]")
    )

    builder = pyspark.sql.SparkSession.builder.config(conf=conf)
    return (configure_spark_with_delta_pip(builder).getOrCreate())

def extract_keywords(text):
    try:
        kw_phrases = keywords.keywords(text, words=3, split=True)
        kw_list = []
        for phrase in kw_phrases:
            kw_list.extend(phrase.split())
        return kw_list
    except Exception as e:
        print(f"Keyword extraction error: {e}")
        return []
    
def process_data():
    spark = spark_configure(user = USER, name_operation='cleaning_wikipedia_text')

    json_path = "data/wikipedia_qa_pairs_full_content.json"
    df = spark.read.option("multiLine", "true").json(json_path)

    df_exploded = df.withColumn("qa_pair", explode(col("qa_pairs"))) \
                .select(
                    col("article_title"),
                    col("article_content"),
                    col("category"),
                    col("qa_pair.q").alias("question"),
                    col("qa_pair.a").alias("answer")
                )

    #df_exploded.show(truncate=False)
    
    # Extracting keyword
    keywords_udf = udf(extract_keywords, ArrayType(StringType()))
    df_exploded = df_exploded.withColumn("keywords", keywords_udf(col("question")))

    # Updating df to match schema
    df_exploded = df_exploded\
        .withColumn("source", lit("wikipedia"))\
        .withColumn("subreddit", lit(None).cast(StringType()))\
        .withColumn("id", hash("question"))\
        .withColumn("author", lit(None).cast(StringType()))\
        .withColumn("upvotes", lit(None).cast(LongType()))\
        .withColumn("num_comments", lit(None).cast(LongType()))\
        .withColumn("created_utc_ts", lit(None).cast(TimestampType()))\
        .withColumn("incorrect_answers", lit(None).cast(ArrayType(StringType())))\
        .withColumnRenamed("category", "topic")\
        .withColumnRenamed("answer", "correct_answer")\
        .withColumnRenamed("question", "title")\
        .drop("article_title", "article_content")
    
    print(df_exploded.head(1))
    gcs_path = "gs://group-1-delta-lake-lets-talk/trusted_zone/wikipedia"

    # Write DataFrame to GCS in Delta format
    df_exploded.write.format("delta").mode("overwrite").save(gcs_path)
    print("Delta table Wiki written to GCS successfully.")

if __name__ == "__main__":
    process_data()
