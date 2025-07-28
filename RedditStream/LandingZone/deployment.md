1. `cd RedditStream/LandingZone`
2. `docker build -t gcr.io/calm-depot-454710-p0/reddit-daily-top:latest .`
3. `gcloud auth configure-docker`
4. `docker push gcr.io/calm-depot-454710-p0/reddit-daily-top:latest`
5. Deployment in the CLI (we did it in the console):
   ```bash
   gcloud run deploy reddit-daily-top \
    --image gcr.io/calm-depot-454710-p0/reddit-daily-top:latest \
    --platform managed \
    --region europe-southwest1 \
    --allow-unauthenticated
   ```
6. Set up scheduling in Cloud Scheduler