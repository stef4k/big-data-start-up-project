## BigQuery with scheduled query

### Enable BigQuery API
- gcloud services enable bigquery.googleapis.com
- gcloud services enable biglake.googleapis.com

### In BigQuery Console
1. In the BigQuery UI
    - Create a Dataset
    - Open the BigQuery console
    - Click + Create Dataset
    - Dataset ID: staging
    - Data location: EU (europe-southwest1)
    - Click Create
2. Load Firestore exports: for each of the three collections (private_users, public_users, tech_dummy):
    - Click + Create Table
    - Create table from: Google Cloud Storage
    - Select or enter GCS URI pattern, e.g.:
        ```bash
        gs://group-1-landing-lets-talk/users/*/all_namespaces/kind_private_users/output-*
        File format: Cloud Datastore Backup
        ```
    - Project: calm-depot-454710-p0
    - Dataset: staging
    - Table name: private_users (or public_users / tech_dummy)
    - Repeat for the other two collections

3. Load Reddit delta table (also repeat this step for Stack Exchange, Wikipedia, and Trivia):
    - Click + Create Table
    - Create table from: Google Cloud Storage
    - Select or enter GCS URI pattern, e.g.:
        ```bash
        gs://group-1-delta-lake-lets-talk/trusted_zone/reddit/
        ```
    - File format: Delta Lake
    - Project: calm-depot-454710-p0
    - Dataset: staging
    - Table name: reddit_posts
    - External table

### Create Scheduled Query
- In the BigQuery UI, open Scheduled queries + Schedule
- Name: refresh_user_topic_keywords (same as the one in scripts_trusted_zone/User-to-Topic-to-Keyword-BigQuery.sql)
Target dataset: calm-depot-454710-p0.landing_eusw1
- Frequency: Daily
- Time: e.g. 00:30 UTC (to let the Cloud Scheduler exports land)
- Destination: leave as “Use the query text”