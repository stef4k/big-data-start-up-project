import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, explode, count, avg, expr

SERVICE_ACCOUNT_KEY  = "C:\\Users\\Kristof\\.gcs\\calm-depot-454710-p0-4cf918c71f69.json"
TRUSTED_DELTA_PATH   = "gs://group-1-delta-lake-lets-talk/trusted_zone/reddit"

def spark_configure(app_name: str):
    conf = (
        pyspark.conf.SparkConf()
          .setAppName(app_name)
          .setMaster("local[*]")
          .set("spark.sql.catalog.spark_catalog",     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .set("spark.sql.extensions",                "io.delta.sql.DeltaSparkSessionExtension")
          .set("spark.hadoop.fs.gs.impl",             "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
          .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
          .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY)
          .set("spark.hadoop.fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
          .set("spark.jars",
               "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
          .set("spark.sql.shuffle.partitions", "4")
    )
    builder = pyspark.sql.SparkSession.builder.config(conf=conf)
    return configure_spark_with_delta_pip(builder).getOrCreate()

if __name__ == "__main__":
    spark = spark_configure("inspect_trusted")
    df    = spark.read.format("delta").load(TRUSTED_DELTA_PATH)

    print("=== Schema ===")
    df.printSchema()

    total = df.count()
    print(f"Total questions: {total}")

    print("\nQuestions per topic:")
    df.groupBy("topic").count().orderBy("count", ascending=False).show(truncate=False)

    print("\nSample rows:")
    df.select("source","subreddit","topic","title","keywords","upvotes")\
      .show(10, truncate=80)

    spark.stop()