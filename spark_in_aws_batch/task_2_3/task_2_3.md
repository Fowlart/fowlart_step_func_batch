### Task 2 — `.whl` Package Baked into Docker Image

**Goal:** Structure the sandbox job as an installable Python package and invoke it as a module entry point.

**Steps:**
- Create the package layout:
  ```
  mdm_spark/
    __init__.py
    jobs/
      __init__.py
      job1_ingest_normalize.py   ← reads incoming_lawyers.json, logs record count and schema
  ```
- `job1_ingest_normalize.py` reads `synthetic_data/incoming_lawyers.json` from S3 (path via env var `S3_INPUT_PATH`), prints schema and row count, then stops — no transformation needed yet
- Add `pyproject.toml` so the package is buildable with `python -m build`
- Build the `.whl` locally: `python -m build --wheel`
- Update the `Dockerfile` to `COPY dist/mdm_spark-*.whl /tmp/` and `RUN pip install /tmp/mdm_spark-*.whl`
- Update the Batch job definition `command` to `["python", "-m", "mdm_spark.jobs.job1_ingest_normalize"]`
- Push to ECR and run a Batch job

**Done when:** CloudWatch logs show the job was invoked as `python -m mdm_spark.jobs.job1_ingest_normalize` and printed the correct row count (10) and column names from `incoming_lawyers.json`.

---

### Task 3 — SSM Parameter Store at Container Startup

**Goal:** Fetch runtime configuration from SSM inside the container instead of hardcoding values.

**Steps:**
- Create two SSM parameters:
  - `/mdm/s3_bucket` (String) — value: the S3 bucket name where `synthetic_data/` is stored
  - `/mdm/pg_url` (SecureString) — value: a placeholder string `jdbc:postgresql://placeholder/mdm` (not used yet, but mirrors the production secret)
- Add `ssm:GetParameter` (with `WithDecryption`) to the Batch job IAM role
- In `job1_ingest_normalize.py`, fetch both parameters via `boto3` before constructing `SparkSession`; build the `incoming_lawyers.json` S3 path from the fetched bucket name
- Remove the `S3_INPUT_PATH` env var from Task 2 — the path is now assembled from SSM values and `BATCH_ID`
- Log the resolved bucket name and the full input path to CloudWatch
- Verify that removing the `ssm:GetParameter` permission causes a clear `AccessDeniedException` in logs before the job proceeds

**Done when:** Job reads the bucket name from SSM, constructs `s3://<bucket>/synthetic_data/incoming_lawyers.json`, loads the file, and logs row count 10; the permission-denied path is confirmed.

---

