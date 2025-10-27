# 🚀 Zoho SalesIQ → GCS + BigQuery ETL Pipeline

### Overview
This pipeline automates the extraction of **Zoho SalesIQ chat data**, stores it in **Google Cloud Storage**, and loads it into **BigQuery** for analysis.  
It also manages incremental upserts and merges using stored procedures for clean, analytics-ready tables.

---

## 🧱 Architecture

```
Cloud Scheduler (daily trigger)
        ↓
   Cloud Run (Flask microservice)
        └── main.py
              ├── Fetch Zoho data
              ├── Export as JSONL
              ├── Upload to GCS
              ├── Load into BigQuery staging tables
              ├── Run stored procedures (MERGE)
              └── Notify via Slack
```

---

## ⚙️ Environment Variables

| Variable | Description |
|-----------|-------------|
| `GOOGLE_CLOUD_PROJECT` | GCP project ID |
| `BQ_DATASET` | BigQuery dataset name (e.g. `zoho_desk`) |
| `GCS_BUCKET` | GCS bucket URI (e.g. `gs://salesiq_lake`) |
| `SALESIQ_CLIENT_ID` | Zoho OAuth client ID |
| `SALESIQ_CLIENT_SECRET` | Zoho OAuth client secret |
| `SALESIQ_REFRESH_TOKEN` | Zoho refresh token |
| `SALESIQ_DEPARTMENT_ID` | SalesIQ department ID |
| `SALES_IQ_BASE_URL` | Base API URL (`https://salesiq.zoho.com`) |
| `API_VERSION` | API version (e.g. `v2`) |
| `SCREEN_NAME` | SalesIQ screen/portal name |
| `SLACK_WEBHOOK` | Slack webhook URL for alerts |
| `PORT` | Server port (default: 8080) |

> 📘 For local development, store these in a `.env` file (not committed).

---

## 🪜 Prerequisites

1. **Google Cloud Project** with the following APIs enabled:  
   - Cloud Run  
   - Cloud Scheduler  
   - BigQuery  
   - Cloud Storage  

2. **Service Account** with roles:  
   - `roles/bigquery.dataEditor`  
   - `roles/storage.objectAdmin`  
   - `roles/run.admin`  
   - `roles/cloudscheduler.admin`  
   - `roles/iam.serviceAccountUser`  

3. **Slack Webhook** created for your workspace (optional but recommended).  

4. **Authenticated gcloud session** for local testing:  
   ```bash
   gcloud auth application-default login
   ```

---

## 🧩 Database Setup (DDL)

Before running the ETL, create the required **BigQuery tables and procedures**.

### 1️⃣ Create the staging and target tables
Run these scripts in the BigQuery editor or CLI:
```bash
bq query --use_legacy_sql=false < salesiq_conversations.sql
bq query --use_legacy_sql=false < salesiq_tags.sql
```
These scripts create:
- `salesiq_conversationDelta` and `salesiq_conversations`
- `salesiq_conversation_tagsDelta` and `salesiq_conversation_tags`

### 2️⃣ Create stored procedures
Run:
```bash
bq query --use_legacy_sql=false < spUpsertConversations.sql
bq query --use_legacy_sql=false < spUpsertTagDetails.sql
```
These enable idempotent incremental merges into the final tables.

---

## 🧪 Local Development

### 1️⃣ Setup environment
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2️⃣ Configure `.env`
```env
GOOGLE_CLOUD_PROJECT=my_project_id
BQ_DATASET=zoho_desk
GCS_BUCKET=gs://salesiq_lake
SLACK_WEBHOOK=https://hooks.slack.com/services/XXX/YYY/ZZZ
# plus all SalesIQ auth vars...
```

### 3️⃣ Run the ETL locally
Start the Flask server:
```bash
gunicorn -b 0.0.0.0:8080 -k gthread --threads 8 --timeout 3600 app:app
```

Trigger a run:
```bash
curl -X POST http://localhost:8080/run
```

---

## 🐳 Running with Docker

From the **repo root**:
```bash
make -f ops/Makefile build
make -f ops/Makefile run
```

Or using Compose:
```bash
make -f ops/Makefile up
curl -X POST http://localhost:8080/run
```

> Full ops instructions are in [`ops/README-ops.md`](ops/README-ops.md).

---

## 🧾 Logging

Each pipeline stage logs start, end, and duration in milliseconds:
```
INFO:salesiq:START export-chats
INFO:salesiq:END   export-chats duration_ms=30123
```
Failures are logged with full tracebacks and Slack alerts.

---

## 💬 Slack Notifications

| Event | Example |
|--------|----------|
| ✅ Success | `SalesIQ export complete | chats=500 | tags=200` |
| ❌ Failure | `BigQuery load failed | table=salesiq_conversationDelta` |
| ℹ️ No data | `Zero-run (no new conversations)` |

---

## ☁️ Cloud Deployment

### Deploy to Cloud Run
```bash
gcloud run deploy salesiq-job   --source .   --region europe-west1   --no-allow-unauthenticated   --memory 1Gi   --timeout 3600   --set-env-vars GOOGLE_CLOUD_PROJECT=gold-courage-194810,BQ_DATASET=zoho_desk,GCS_BUCKET=gs://salesiq_lake
```

### Trigger via Cloud Scheduler
```bash
gcloud scheduler jobs create http salesiq-daily   --schedule="0 7 * * *"   --timezone="Africa/Lagos"   --http-method=POST   --uri="https://<CLOUD-RUN-URL>/run"   --oidc-service-account-email=<SERVICE_ACCOUNT>@<PROJECT>.iam.gserviceaccount.com   --oidc-token-audience="https://<CLOUD-RUN-URL>/run"
```

---

## 🧠 Summary

| Component | Purpose |
|------------|----------|
| **Flask App** | Handles `/run` trigger and orchestrates ETL |
| **GCS** | Temporary landing zone for chat JSONL files |
| **BigQuery** | Final analytics layer (Delta + Production tables) |
| **Stored Procedures** | Handle upserts and deduplication |
| **Slack Notifier** | Sends alerts for job success/failure |


### 🪣 GCS Bucket

All exported JSONL files are stored in a Google Cloud Storage bucket named **`salesiq_lake`**.  
Optionally, you can enable a lifecycle rule to **auto-delete files older than 30 days** to manage storage costs.

---

### 📚 Reference

**SalesIQ REST API Documentation**  
[Developer docs](https://www.zoho.com/salesiq/help/developer-section/rest-api-prerequisite-v2.html)
[Video guide to generating Oauth token](https://drive.google.com/file/d/1t6rPbA6K8MWJqLrP-qh9I2P0bCoyTNnr/view?usp=sharing)


### 🏁 End Result
A fully automated, serverless ETL pipeline that extracts Zoho SalesIQ chat and tag data, loads it into BigQuery, and maintains clean, deduplicated datasets ready for reporting and modeling.
