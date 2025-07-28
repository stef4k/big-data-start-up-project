import requests
import json
from google.cloud import storage
import os
import time,random

def fetch_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

def save_to_file(data, filename):
    filepath = os.path.join("./json", filename)
    with open(filepath, "w") as file:
        json.dump(data, file, indent=4)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name, credentials_path):
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}")

if __name__ == "__main__":
    API_URL = "https://opentdb.com/api.php?amount=50"
    FILE_NAME = "trivia_questions.json"
    BUCKET_NAME = "group-1-landing-lets-talk"
    DESTINATION_BLOB_NAME = "trivia_questions/trivia_questions.json"
    CREDENTIALS_PATH = "env/key.json"
    FILE_LOCATION = "json"

    data = fetch_data(API_URL)
    # Use loop to get more values since there is a limit of 50 values per API CALL
    for i in range(800):
        sleep_time = random.randint(5, 8)
        print(f"Sleeping for {sleep_time} seconds.")
        time.sleep(sleep_time)
        new_data = fetch_data(API_URL)
        data["results"].extend(new_data["results"])
    save_to_file(data, FILE_NAME)
    upload_to_gcs(BUCKET_NAME, os.path.join(FILE_LOCATION, FILE_NAME), DESTINATION_BLOB_NAME, CREDENTIALS_PATH)