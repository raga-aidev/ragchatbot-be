# RAG Chatbot Backend

## Setup

1. Set environment variables:
   ```bash
   export GOOGLE_CLOUD_PROJECT=your-project-id
   export GOOGLE_API_KEY=your-gemini-api-key
   ```

2. Authenticate with GCP:
   ```bash
   gcloud auth application-default login
   ```

3. Build and run:
   ```bash
   ./gradlew build
   ./gradlew bootRun
   ```

Server runs on http://localhost:8080

