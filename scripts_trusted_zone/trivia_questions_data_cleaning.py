import pyspark
from pyspark.sql.functions import col, lower, regexp_replace, udf
from google.cloud import storage
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, lit, hash
from pyspark.sql.types import ArrayType, StringType, LongType, TimestampType
from summa import keywords
import os
import html

# To avoid python3 executable error
os.environ["PYSPARK_PYTHON"] = "python"
USER = 'stef4'
BUCKET_PATH = "gs://group-1-landing-lets-talk/trivia_questions/trivia_questions.json"


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
        .setMaster("local[1]")
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

# HTML character removal function
def html_unescape(text):
    return html.unescape(text)

def process_data():
    spark = spark_configure(user = USER, name_operation='cleaning_trivia_questions')

    df_full = spark.read.option("multiline", "true").json(BUCKET_PATH)

    # Extract only the "results" array
    df_results = df_full.selectExpr("explode(results) as question") \
        .select("question.*")

    # Deduplication by 'question'
    df_results = df_results.dropDuplicates(["question"])

    # Constraint Validation: remove rows with null questions or correct answers
    df_results = df_results.filter(col("question").isNotNull() & col("correct_answer").isNotNull())

    # New column with text normalized 
    df_results = df_results.withColumn("question_clean", lower(regexp_replace(col("question"), "[^a-zA-Z0-9\\s]", "")))

    # Extracting keyword
    keywords_udf = udf(extract_keywords, ArrayType(StringType()))
    df_results = df_results.withColumn("keywords", keywords_udf(col("question_clean")))

    # Remove html tags
    unescape_udf = udf(html_unescape, StringType())
    df_results = df_results.withColumn("question", unescape_udf(df_results["question"]))


    # New columns and renaming to follow schema
    df_results = df_results\
        .withColumn("source", lit("trivia"))\
        .withColumn("subreddit", lit(None).cast(StringType()))\
        .withColumn("id", hash("question"))\
        .withColumn("author", lit(None).cast(StringType()))\
        .withColumn("upvotes", lit(None).cast(LongType()))\
        .withColumn("num_comments", lit(None).cast(LongType()))\
        .withColumn("created_utc_ts", lit(None).cast(TimestampType()))\
        .withColumnRenamed("question", "title")\
        .withColumnRenamed("category", "topic")\
        .drop("type", "difficulty", "question_clean")
        

    print(df_results.head(1))

    gcs_path = "gs://group-1-delta-lake-lets-talk/trusted_zone/trivia"

    # Write DataFrame to GCS in Delta format
    df_results.write.format("delta").mode("overwrite").save(gcs_path)

    print("Delta table Trivia Questions written to GCS successfully.")

if __name__ == "__main__":
    process_data()