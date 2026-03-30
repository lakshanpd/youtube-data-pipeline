# 01_build_data_ingestion_workflow

## 📌 Objective

In this stage, the goal is to design and implement a **production-style data ingestion workflow** for collecting YouTube data related to a specific day in Sri Lanka.

The focus is on:

* Building a **scalable ingestion pipeline**
* Following **data engineering best practices (ELT)**
* Ensuring **raw data is preserved**
* Introducing **metadata tracking (data catalog)**
* Adding **logging for observability**
* Adding **workflow orchestration for scheduling and managing pipeline execution**

---

## 🧠 Key Design Principles

* **ELT over ETL** → Store raw data first, transform later
* **Separation of concerns** → API, storage, metadata, logging are modular
* **Idempotency** → Avoid duplicate processing
* **Traceability** → Every ingestion run is tracked

---

## 🔄 Data Ingestion Workflow

The ingestion process follows a **batch-based iterative approach**:

### Step 1: Fetch Search API Results

* Call YouTube Search API with:

  * region = Sri Lanka (LK)
  * date range (specific day)
  * keyword-based queries
* Retrieve a batch (max 50 results)

### Step 2: Store Raw Search Data

* Store the **exact API response** in the data lake (MinIO)
* No transformation is applied

---

### Step 3: Extract Video IDs

* Parse search response
* Extract `videoId` values
* Store extracted IDs (optional but recommended for tracking)

---

### Step 4: Fetch Video Details

* Call Videos API using batch of video IDs (max 50)
* Retrieve:

  * snippet
  * statistics
  * contentDetails

---

### Step 5: Store Raw Video Data

* Store full API response in MinIO
* Maintain raw JSON format

---

### Step 6: Iterate

* Continue:

  * pagination (nextPageToken)
  * multiple keyword queries
* Stop when:

  * quota limit reached OR
  * target number of videos reached

---

## 🧱 Workflow Diagram

```
Search API → Store Raw Search
        ↓
   Extract Video IDs
        ↓
   Videos API → Store Raw Videos
        ↓
      Repeat
```

---

## 🗂️ Data Storage Strategy (MinIO)

Data is stored in a **partitioned structure**:

```
raw/
  youtube/
    date=YYYY-MM-DD/
      run_id=<run_id>/
        source=search/
          batch_001.json
        source=videos/
          batch_001.json
```

---

## 🧾 Data Catalog (Metadata Tracking)

A metadata layer is implemented using PostgreSQL to track ingestion.

### Tables:

#### 1. ingestion_runs

* Tracks each pipeline execution
* Fields:

  * run_id
  * run_date
  * status
  * start_time, end_time
  * total_records

#### 2. data_files

* Tracks each stored file
* Fields:

  * file_path
  * source (search / videos)
  * record_count
  * run_id

#### 3. video_ids_tracking (optional)

* Tracks processed video IDs
* Helps avoid duplicates and reprocessing

---

## 📊 Logging Strategy

Structured logging is used for observability.

### Log Format

```
timestamp | level | service | run_id | message
```

### Example Logs

```
INFO | run started | run_id=123
INFO | search API success | keyword=music
INFO | stored search batch | records=50
INFO | fetched video batch | size=50
ERROR | API failure | retrying
```

Logs are:

* written to file
* printed to console
* designed to integrate with centralized logging later

---

## 📁 Project Structure (Relevant to Ingestion)

```
src/
  ingestion/
    search_api.py
    video_api.py
    extractor.py
    orchestrator.py

  storage/
    minio_client.py

  metadata/
    db.py
    catalog.py

  logging/
    logger.py

configs/
  config.yaml

dags/
  (Airflow integration)
```

---

## ⚙️ Execution Flow (Code Level)

```
orchestrator.py
    ↓
fetch_search_results()
    ↓
store_raw_data()
    ↓
extract_video_ids()
    ↓
fetch_video_details()
    ↓
store_raw_data()
    ↓
update_catalog()
```
