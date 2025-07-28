### Pub/Sub Deployment (UI)
- Go to the Pub/Sub section.
- Click Topics -> Create Topic.
- Enter Name: reddit-landing-new -> Create.

### Configure GCS notifications
```
gsutil notification create \
  -t reddit-landing-new \
  -f json \ # payload format
  -e OBJECT_FINALIZE \ # event type
  -p reddit/daily_top_posts/ \ # prefix filter
  gs://group-1-landing-lets-talk
```
Verify with:
```
gsutil notification list gs://group-1-landing-lets-talk
```

### Configure Cloud Function
1. `cd RedditStream/TrustedZone`
2. `docker build -t gcr.io/calm-depot-454710-p0/reddit-to-trusted:latest .`
3. `gcloud auth configure-docker`
4. `docker push gcr.io/calm-depot-454710-p0/reddit-to-trusted:latest`
5. Deployment in the CLI (we did it in the console):
    ```bash
    gcloud run deploy reddit-to-trusted \
      --image gcr.io/calm-depot-454710-p0/reddit-to-trusted:latest \
      --platform managed \
      --region europe-west1 \
      --allow-unauthenticated \
      --set-env-vars \
         INPUT_BUCKET=group-1-landing-lets-talk, \
         OUTPUT_BUCKET=group-1-delta-lake-lets-talk, \
         OUTPUT_PREFIX=trusted_zone/reddit
    ```
### Eventarc Trigger
1. Go to Eventarc in the console.
2. Click Create Trigger, name: tr-reddit-to-trusted
3. Event provider: Cloud Pub/Sub
4. Event type: google.cloud.pubsub.topic.v1.messagePublished
5. Same region! -> europe-southwest1
6. Destination platform: Cloud Run

What we have now:
1. Cloud Run service reddit-daily-top
2. Cloud Scheduler job reddit-daily-job -> cron 0 0 * * * hitting service URL

### Verify (PowerShell)
1. Trigger the collector on demand
```powershell
Invoke-RestMethod -Uri "https://reddit-daily-top-<hash>.europe-southwest1.run.app/"
```
2. Manually emit a Pub/Sub message for that object
```powershell
# Replace <JSON_PATH> with the path returned above, has to be a daily folder
# Example: reddit/daily_top_posts/2025-05-23/reddit_daily_top_posts.json
$inner = @{
  bucket = "group-1-landing-lets-talk"
  name   = "<JSON_PATH>"
} | ConvertTo-Json -Compress
$innerB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($inner))

$pubsub = @{ message = @{ data = $innerB64 } } | ConvertTo-Json -Compress

gcloud pubsub topics publish reddit-landing-new --message="$pubsub"
```
3. Direct call to trusted service (bypass Pub/Sub)
````powershell
# create payload.json with the same {"bucket": "...", "name": "..."} body
$raw = Get-Content payload.json -Raw
$rawB64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($raw))
@{ message = @{ data = $rawB64 } } | ConvertTo-Json -Compress |
  curl.exe -H "Content-Type: application/json" --data-binary @- `
    (gcloud run services describe reddit-to-trusted --region=europe-west1 --format="value(status.url)")
````
Should return 200 on success.

### Fix timezone edge case
- When Madrid is UTC+2 (summer), that is 22:00 UTC -> the collector writes previous-day date.
- When UTC+1 (winter), it runs 23:00 UTC -> still previous-day.
- Fix: ```powershell
# change to UTC but keep the same cron
gcloud scheduler jobs update http reddit-daily-job `
  --location europe-west3 `
  --schedule "0 0 * * *" `
  --time-zone "UTC"
```