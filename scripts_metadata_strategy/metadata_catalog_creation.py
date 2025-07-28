import pandas as pd
from google.cloud import storage
import os


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, credentials_path):
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")



# Metadata rows as dictionaries
metadata = [
    # Landing zone
    {
        "source": "reddit", "zone": "landing", "file_name": "reddit_posts.json",
        "path_or_target": "gs://group-1-landing-lets-talk/reddit/", "owner": "ingestion_team",
        "description": "Raw Reddit questions ingested via dockerized python script", "update_frequency": "daily",
        "tags": ["questions", "reddit", "raw"], "data_quality_score": 30
    },
    {
        "source": "stack_exchange", "zone": "landing", "file_name": "stackexchange_posts.json",
        "path_or_target": "gs://group-1-landing-lets-talk/stack_exchange/", "owner": "ingestion_team",
        "description": "Raw Stack Exchange questions", "update_frequency": "none",
        "tags": ["questions", "stackexchange", "raw"], "data_quality_score": 40
    },
    {
        "source": "trivia", "zone": "landing", "file_name": "trivia_questions.json",
        "path_or_target": "gs://group-1-landing-lets-talk/trivia_questions/", "owner": "ingestion_team",
        "description": "Raw Trivia questions ingested via API", "update_frequency": "none",
        "tags": ["questions", "trivia", "raw"], "data_quality_score": 42
    },
    {
        "source": "wiki_films", "zone": "landing", "file_name": "film_pages.json",
        "path_or_target": "gs://group-1-landing-lets-talk/wikipedia/", "owner": "ingestion_team",
        "description": "Raw Wikipedia film text", "update_frequency": "none",
        "tags": ["films", "wiki", "raw"], "data_quality_score": 25
    },
    {
        "source": "wiki_sport", "zone": "landing", "file_name": "sport_pages.json",
        "path_or_target": "gs://group-1-landing-lets-talk/wikipedia/", "owner": "ingestion_team",
        "description": "Raw Wikipedia sport pages", "update_frequency": "none",
        "tags": ["sports", "wiki", "raw"], "data_quality_score": 25
    },
    {
        "source": "wiki_tech", "zone": "landing", "file_name": "tech_pages.json",
        "path_or_target": "gs://group-1-landing-lets-talk/wikipedia/", "owner": "ingestion_team",
        "description": "Raw Wikipedia tech pages", "update_frequency": "none",
        "tags": ["tech", "wiki", "raw"], "data_quality_score": 25
    },
    {
        "source": "users", "zone": "landing", "file_name": "users",
        "path_or_target": "gs://group-1-landing-lets-talk/users/", "owner": "ingestion_team",
        "description": "Users data", "update_frequency": "daily",
        "tags": ["users"], "data_quality_score": 75
    },

    # Trusted zone
    {
        "source": "reddit", "zone": "trusted", "file_name": "reddit_questions_delta",
        "path_or_target": "gs://group-1-delta-lake-lets-talk/trusted_zone/reddit/", "owner": "data_engineering",
        "description": "Cleaned Reddit questions", "update_frequency": "daily",
        "tags": ["clean", "reddit", "questions"], "data_quality_score": 80
    },
    {
        "source": "wiki_questions", "zone": "trusted", "file_name": "wiki_generated_questions_delta",
        "path_or_target": "gs://group-1-delta-lake-lets-talk/trusted_zone/wikipedia/", "owner": "data_engineering",
        "description": "Synthetic questions generated from Wikipedia content", "update_frequency": "none",
        "tags": ["clean", "wiki", "questions"], "data_quality_score": 85
    },
    {
        "source": "stack_exchange", "zone": "trusted", "file_name": "stack_exchange_delta",
        "path_or_target": "gs://group-1-delta-lake-lets-talk/trusted_zone/stack_exchange/", "owner": "data_engineering",
        "description": "Cleaned stack exchange questions", "update_frequency": "none",
        "tags": ["clean", "stack_exchange", "questions"], "data_quality_score": 85
    },
    {
        "source": "trivia", "zone": "trusted", "file_name": "trivia_delta",
        "path_or_target": "gs://group-1-delta-lake-lets-talk/trusted_zone/trivia/", "owner": "data_engineering",
        "description": "Trivia questions deduplicated and organized", "update_frequency": "none",
        "tags": ["clean", "trivia", "questions"], "data_quality_score": 95
    },
    {
        "source": "users", "zone": "trusted", "file_name": "users_topic",
        "path_or_target": "gs://group-1-delta-lake-lets-talk/trusted_zone/user_topic_keywords/", "owner": "data_engineering",
        "description": "Users, their topic and keywords preferences", "update_frequency": "daily",
        "tags": ["clean", "users", "topic"], "data_quality_score": 90
    },

    # Exploitation zone
    {
        "source": "all_elements_together", "zone": "exploitation", "file_name": "neo4j_graph",
        "path_or_target": "neo4j+s://06b06d8f.databases.neo4j.io", "owner": "graph_analysis_team",
        "description": "Graph representation of all questions, related topics, keywords and users", "update_frequency": "none",
        "tags": ["graph", "questions", "users"], "data_quality_score": 98
    }
]

# Convert to pandas DataFrame
df = pd.DataFrame(metadata)
df.to_json("json/metadata_catalog.json", orient="records", lines=True)


FILE_NAME = "metadata_catalog.json"
BUCKET_NAME = "group-1-landing-lets-talk"
DESTINATION_BLOB_NAME = "metadata_catalog/catalog.json"
CREDENTIALS_PATH = "env/key.json"
FILE_LOCATION = "json"

upload_to_gcs(BUCKET_NAME, os.path.join(FILE_LOCATION, FILE_NAME), DESTINATION_BLOB_NAME, CREDENTIALS_PATH)
