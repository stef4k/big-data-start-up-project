1. Go to the Cloud Console:
    - Navigate to Cloud Functions.
    - Click Create Function.
2. Configure the Function:
    - Name: `firebase-scheduled-export`
    - Runtime: `Node.js`
    - Entry Point: `scheduledFirebaseExport`
    - Source Code:
        - Inline Editor or
        - Upload a ZIP file containing the code.
    - Deploy
3. Set up scheduling in Cloud Scheduler
