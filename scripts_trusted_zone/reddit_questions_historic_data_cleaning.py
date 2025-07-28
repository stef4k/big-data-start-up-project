"""
For this code to work, we need python 3.11.8, PySpark 3.5.* Delta Lake 2.4.*
Refer to: https://stackoverflow.com/questions/77369508/python-worker-keeps-on-crashing-in-pyspark

To tweak the Spark configuration:
  If you don't have conda, get it from here https://repo.anaconda.com/miniconda/
    version: Miniconda3-py311_*
  conda create --prefix D:/python_envs/py3118 python=3.11.8 -y
  conda activate D:/python_envs/py3118
  pip install pyspark==3.5.* delta-spark==3.3.* numpy

Run the code when this is done
"""

import pyspark
from delta import configure_spark_with_delta_pip

from pyspark.sql.functions import (
    col, lower, regexp_replace,
    from_unixtime, to_timestamp,
    explode, lit,
    when, length, size, udf, expr
)
from pyspark.sql.types import ArrayType, StringType

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer, StopWordsRemover,
    NGram, SQLTransformer,
    CountVectorizer, IDF
)
from pyspark.ml.linalg import SparseVector

# run nltk.download("stopwords") before running this code
import nltk
from nltk.corpus import stopwords

SERVICE_ACCOUNT_KEY = "C:\\Users\\Kristof\\.gcs\\calm-depot-454710-p0-4cf918c71f69.json"
RAW_JSON_PATH       = "gs://group-1-landing-lets-talk/reddit/reddit_posts.json"
TRUSTED_DELTA_PATH  = "gs://group-1-delta-lake-lets-talk/trusted_zone/reddit"
TOP_N               = 4

SUBREDDITS = [
    "AskReddit", "NoStupidQuestions", "ExplainLikeImFive", "questions", "ask",
    "DoesAnybodyElse", "askscience", "Ask_Politics", "TrueAskReddit",
    "AskScienceFiction", "AskEngineers", "AskHistorians", "AskMen", "AskWomen"
]

# map subreddit to a main topic
TOPIC_MAP = {
    "AskReddit":           "General Chat",
    "NoStupidQuestions":   "Advice & Life",
    "ExplainLikeImFive":   "Science",
    "askscience":          "Science",
    "AskHistorians":       "History",
    "AskMen":              "Social & Culture",
    "AskWomen":            "Social & Culture",
    "Ask_Politics":        "Social & Culture",
    "AskScienceFiction":   "Entertainment & Fiction",
    "AskEngineers":        "Engineering",
    "DoesAnybodyElse":     "General Chat",
    "questions":           "General Chat",
    "ask":                 "General Chat",
    "TrueAskReddit":       "General Chat"
}

# Spark config
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

# landing -> trusted pipeline
if __name__ == "__main__":
    spark = spark_configure("reddit_to_trusted")

    # read landing zone data
    df_raw = spark.read.option("multiline", "true").json(RAW_JSON_PATH)

    # explode + standardize
    exploded = []
    for sub in SUBREDDITS:
        part = (
            df_raw
              .select(explode(col(sub)).alias("post"))
              .select(
                  lit(sub).alias("subreddit"),
                  col("post.data.id").alias("id"),
                  col("post.data.author").alias("author"),
                  col("post.data.title").alias("title"),
                  col("post.data.selftext").alias("selftext"),
                  col("post.data.created_utc").alias("created_utc"),
                  col("post.data.num_comments").alias("num_comments"),
                  col("post.data.score").alias("score"),
                  col("post.data.upvote_ratio").alias("upvote_ratio")
              )
        )
        exploded.append(part)


    from functools import reduce
    # title_clean and selftext_clean is needed later for keyword extraction
    df_clean = reduce(lambda a, b: a.union(b), exploded) \
        .select(
            "subreddit","id","author","title","selftext",
            "created_utc","num_comments","score","upvote_ratio"
        ) \
        .dropDuplicates(["id"]) \
        .filter(
            col("id").isNotNull() &
            col("author").isNotNull() &
            col("title").isNotNull()
        ) \
        .withColumn(
            "created_utc_ts",
            to_timestamp(from_unixtime(col("created_utc")), "yyyy-MM-dd HH:mm:ss")
        ) \
        .withColumn("title_clean",
            lower(regexp_replace(col("title"), "[^a-zA-Z0-9\\s]", ""))) \
        .withColumn("selftext_clean",
            lower(regexp_replace(col("selftext"), "[^a-zA-Z0-9\\s]", "")))
    
    #################################### KEYWORD EXTRACTION START ##############################################

    # fallback to title if selftext is empty
    df_kw = df_clean.withColumn(
        "text",
        when(length(col("selftext_clean")) > 0, col("selftext_clean"))
        .otherwise(col("title_clean"))
    )

    # PIPELINE
    tokenizer = RegexTokenizer(inputCol="text", outputCol="tokens_raw", pattern="\\W+")
    # remove stop words: NLTK’s list and a few reddit-specific stuff
    sw = stopwords.words("english")
    extra_sw  = ["reddit","ask","question","thanks","please"]
    remover   = StopWordsRemover(inputCol="tokens_raw", outputCol="tokens",
                                 stopWords=sw + extra_sw)
    # 2-3 word n-grams
    bigram    = NGram(n=2, inputCol="tokens", outputCol="bigrams")
    trigram   = NGram(n=3, inputCol="tokens", outputCol="trigrams")
    assembler = SQLTransformer(statement="""
        SELECT *, array_union(tokens, array_union(bigrams, trigrams)) AS all_grams
        FROM __THIS__
    """)
    cv        = CountVectorizer(inputCol="all_grams", outputCol="rawFeatures",
                                 vocabSize=10_000, minDF=3)
    idf       = IDF(inputCol="rawFeatures", outputCol="features")

    pipeline  = Pipeline(stages=[tokenizer, remover, bigram, trigram, assembler, cv, idf])
    model     = pipeline.fit(df_kw)
    df_feats  = model.transform(df_kw)

    # broadcast vocabulary
    vocab    = model.stages[-2].vocabulary
    bc_vocab = spark.sparkContext.broadcast(vocab)

    # Python UDF that only looks at the sparse vector's indices and values
    def top_terms(sv):
        if not isinstance(sv, SparseVector):
            return []
        iv = list(zip(sv.indices, sv.values))
        # sort desc by weight
        top = sorted(iv, key=lambda x: -x[1])[:TOP_N]
        return [bc_vocab.value[i] for i, w in top if w>0]

    top_udf = udf(top_terms, ArrayType(StringType()))

    # post-filtering to drop short, numeric or generic things, this can be improved,
    # but for the proof of concept, this is good enough
    df_kw2 = df_feats \
    .withColumn("keywords", top_udf(col("features"))) \
    .withColumn("keywords", expr("""
        filter(
        keywords,
        x -> length(x) > 3 AND NOT x RLIKE '^[0-9]+$'
        )
    """)) \
    .filter(size("keywords") > 0) # remove questions without keywords

    ############################## KEYWORD EXTRACTION END ##############################################

    # topic + write to trusted
    bc_topic_map = spark.sparkContext.broadcast(TOPIC_MAP)
    topic_udf    = udf(lambda s: bc_topic_map.value.get(s, "Other"), StringType())

    df_trusted = (
        df_kw2
          .withColumn("source", lit("reddit"))
          .withColumn("topic",  topic_udf(col("subreddit")))
          .withColumn("correct_answer", lit(None).cast(StringType()))
          .withColumn("incorrect_answers", lit(None).cast(ArrayType(StringType())))
          .select(
              "source","subreddit","topic",
              "id","author","title","keywords",
              col("score").alias("upvotes"),
              "num_comments","created_utc_ts",
              "correct_answer", "incorrect_answers"
          )
    )

    df_trusted.write.format("delta")\
        .option("overwriteSchema", "true")\
       .mode("overwrite")\
       .save(TRUSTED_DELTA_PATH)
    print(f"[trusted] Delta table written to: {TRUSTED_DELTA_PATH}")

    spark.stop()