import requests
import json
from google.cloud import storage
import os

def fetch_wikipedia_pages(topic, lang='en'):
    base_url = f'https://{lang}.wikipedia.org/w/api.php'
    params = {
        'action': 'query',
        'format': 'json',
        'list': 'search',
        'srsearch': topic,
        'srlimit': 1000
    }
    
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    search_results = response.json().get('query', {}).get('search', [])
    pages = []
    for result in search_results:
        page_title = result['title']
        page_info = fetch_page_content(page_title, lang)
        pages.append(page_info)
    return pages

def fetch_page_content(title, lang='en'):
    base_url = f'https://{lang}.wikipedia.org/w/api.php'
    params = {
        'action': 'query',
        'format': 'json',
        'titles': title,
        'prop': 'extracts',
        'explaintext': True
    }
    
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    pages_data = response.json().get('query', {}).get('pages', {})
    page = list(pages_data.values())[0]
    return {
        'title': title,
        'content': page.get('extract', '')
    }

def save_to_file(pages, filename):
    filepath = os.path.join("./json", filename)
    with open(filepath, 'w') as file:
        json.dump(pages, file, indent=4)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

if __name__ == '__main__':
    topics = ['Sport', 'Film', 'Tech'] 
    bucket_name = 'group-1-landing-lets-talk'
    destination_blob = "wikipedia/"
    file_location = "json"

    for topic in topics:
        print(f"Fetching pages related to: {topic}")
        pages = fetch_wikipedia_pages(topic)
        file_name = f"{topic.lower()}_pages.json"
        save_to_file(pages, file_name)
        destination_blob_name = destination_blob + file_name
        upload_blob(bucket_name, os.path.join(file_location, file_name),
                      destination_blob_name)
    print(f"Process completed successfully!")
