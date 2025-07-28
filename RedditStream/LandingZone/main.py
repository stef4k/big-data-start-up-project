import json
import os
from datetime import datetime
import requests
from flask import Flask, jsonify
from google.cloud import storage

app = Flask(__name__)

# credentials
CLIENT_ID = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
CLIENT_SECRET = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
USERNAME = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
PASSWORD = 'XXXXXXXXXXXXXXXXXXXXXXXXXX'
USER_AGENT = 'Letstalk'

# bucket
BUCKET_NAME = 'group-1-landing-lets-talk'

def get_reddit_daily_top_posts():
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
    data = {
        'grant_type': 'password',
        'username': USERNAME,
        'password': PASSWORD
    }
    headers = {'User-Agent': USER_AGENT}
    response = requests.post('https://www.reddit.com/api/v1/access_token',
                             auth=auth, data=data, headers=headers)
    token = response.json().get('access_token')
    if not token:
        raise Exception("Could not obtain reddit access token.")
    headers['Authorization'] = f'bearer {token}'

    subreddits = ['AskReddit', 'NoStupidQuestions', 'ExplainLikeImFive', 'questions']
    daily_posts = {}

    for sub in subreddits:
        url = f'https://oauth.reddit.com/r/{sub}/top'
        params = {'limit': 10, 't': 'day'}
        r = requests.get(url, headers=headers, params=params)
        data = r.json()
        daily_posts[sub] = data.get('data', {}).get('children', [])
    return daily_posts

def upload_to_bucket(data_str, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data_str, content_type='application/json')
    return f"gs://{BUCKET_NAME}/{destination_blob_name}"

@app.route('/', methods=['GET'])
def run_job():
    try:
        daily_posts = get_reddit_daily_top_posts()
        today = datetime.utcnow().strftime('%Y-%m-%d')
        destination_blob_name = f"reddit/daily_top_posts/{today}/reddit_daily_top_posts.json"
        json_data = json.dumps(daily_posts, ensure_ascii=False, indent=2)
        uri = upload_to_bucket(json_data, destination_blob_name)
        return jsonify({"status": "success", "outputUri": uri}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
