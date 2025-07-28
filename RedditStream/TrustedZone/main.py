import os
import json
import tempfile
import base64
from flask import Flask, request
from google.cloud import storage
import pandas as pd
from datetime import datetime

app = Flask(__name__)
storage_client = storage.Client()

# your buckets
INPUT_BUCKET   = os.environ.get("INPUT_BUCKET",   "group-1-landing-lets-talk")
OUTPUT_BUCKET  = os.environ.get("OUTPUT_BUCKET",  "group-1-delta-lake-lets-talk")
OUTPUT_PREFIX  = os.environ.get("OUTPUT_PREFIX",  "trusted_zone/reddit/daily_top_posts")

TOPIC_MAP = {
    "AskReddit":"General Chat","NoStupidQuestions":"Advice & Life",
    "ExplainLikeImFive":"Science","askscience":"Science","AskHistorians":"History",
    "AskMen":"Social & Culture","AskWomen":"Social & Culture","Ask_Politics":"Social & Culture",
    "AskScienceFiction":"Entertainment & Fiction","AskEngineers":"Engineering",
    "DoesAnybodyElse":"General Chat","questions":"General Chat","ask":"General Chat",
    "TrueAskReddit":"General Chat"
}

def map_topic(sr):
    return TOPIC_MAP.get(sr, "Other")

@app.route("/", methods=["GET", "POST"])
def run():
    # health check
    if request.method == "GET":
        return ("OK", 200)
    
    print("incoming raw body:", request.get_data(as_text=True))
    envelope = request.get_json(force=True)
    if not envelope:
        return "Bad Request: no JSON payload", 400

    # support raw Pub/Sub AND CloudEvent envelope
    if "message" in envelope:
        # raw Pub/Sub trigger (straight from Pub/Sub)
        data = envelope["message"].get("data")
    elif "data" in envelope and isinstance(envelope["data"], dict):
        # Eventarc CloudEvent wrapper
        data = envelope["data"] \
                   .get("message", {}) \
                   .get("data")
    else:
        return "Bad Pub/Sub message", 400

    # Pub/Sub data is base64; Cloud Run trigger-topic does JSON directly under "message"
    raw = base64.b64decode(data).decode("utf-8")
    print("▶▶ raw decoded payload:", raw)
    # defensively parse only the { "bucket": "...", "name": "..." } payload
    try:
        msg         = json.loads(raw)
        bucket_name = msg["bucket"]
        name        = msg["name"]
    except (json.JSONDecodeError, KeyError) as e:
        # either malformed JSON or missing keys → just ignore
        print(f"Ignoring non-data message ({e}): {raw}")
        return ("Ignored", 200)

    # only process reddit/daily_top_posts/ JSONs
    if not name.startswith("reddit/daily_top_posts/") or not name.endswith(".json"):
        return "Ignored", 200

    # download
    bucket = storage_client.bucket(bucket_name)
    blob   = bucket.blob(name)
    raw    = blob.download_as_string()
    posts  = json.loads(raw)

    # flatten into records
    rows = []
    for sr, items in posts.items():
        for child in items:
            d = child["data"]
            rows.append({
                "source":        "reddit",
                "subreddit":     sr,
                "topic":         map_topic(sr),
                "id":            d.get("id"),
                "author":        d.get("author"),
                "title":         d.get("title"),
                "upvotes":       d.get("score"),
                "num_comments":  d.get("num_comments"),
                "created_utc_ts": datetime.utcfromtimestamp(d.get("created_utc"))
            })

    if not rows:
        return "Empty", 204

    df = pd.DataFrame(rows)

    # write to CSV
    out_name = name.replace("reddit/daily_top_posts/", "").replace(".json", ".csv")
    out_path = f"{OUTPUT_PREFIX}/{out_name}"
    with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
        df.to_csv(tmp.name, index=False)
        out_blob = storage_client.bucket(OUTPUT_BUCKET).blob(out_path)
        out_blob.upload_from_filename(tmp.name, content_type="text/csv")

    return f"Wrote {len(df)} rows to gs://{OUTPUT_BUCKET}/{out_path}", 200

if __name__=="__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",8080)))
