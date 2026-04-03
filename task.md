# MDM Spark Pipeline — Sandbox Learning Tasks

## Concepts Covered

The tasks below replicate the core infrastructure patterns used in the production MDM Lawyer pipeline:

- **Delta Lake on S3** — inter-job storage layer using `delta-spark`; file statistics enable Spark's automatic join strategy selection and data skipping
- **Python package (`.whl`) baked into Docker image** — jobs are module entry points (`python -m pkg.jobs.job1`), not flat scripts; the `.whl` is built in CI and installed at image build time
- **AWS SSM Parameter Store at container startup** — secrets (DB URL, S3 bucket) are fetched via `boto3` at runtime; requires IAM `ssm:GetParameter` on the Batch job role
- **Structured Streaming `foreachBatch` with S3 checkpoint** — Job 2 processes entities in micro-batches; the checkpoint makes retries resume from the last completed micro-batch rather than from scratch; `availableNow=True` makes the stream self-terminating
- **Multi-job Step Functions with `ContainerOverrides`** — `batch_id` and `micro_batch_size` are passed from execution input through `ContainerOverrides.Environment` to each container; each state has `Retry` with exponential backoff and a shared `Catch → PipelineFailed` terminal
- **JDBC read/write to RDS from Batch** — Jobs 1 and 4 use JDBC; requires Batch compute and RDS to share a VPC private subnet; no public internet access
- **Jaro-Winkler UDF + cross-join (Phase 2 candidate search)** — `jellyfish` UDF applied over a `crossJoin` of unmatched entities and cluster names; top-10 per entity selected via `row_number().over(Window.partitionBy(...))`

---

## Tasks

### Task 1 — Delta Lake on S3

**Goal:** Replace the existing Parquet-based S3 read/write job with Delta Lake, using the synthetic cluster data as the write source.

**Input:** `synthetic_data/existing_clusters.json` — 100 lawyer cluster records.

**Steps:**
- Add `delta-spark==3.1.0` to the Dockerfile
- Configure `SparkSession` with Delta extensions and catalog
- Read `existing_clusters.json` from S3 via `spark.read.json(path)`
- Parse `searching_keys` with `from_json` using the schema `{emails: array<string>, linkedin_slugs: array<string>, phones: array<string>, full_names_normalized: array<string>}`
- Explode `emails` into a flat `(master_id, email)` table; write to Delta on S3 with `mode("overwrite")`
- Apply `Z-orderBy("email")` on the written table — this mirrors the production `cluster_emails/` pre-exploded table written by Job 1
- Append 2 extra synthetic rows to the same Delta table using `mode("append")` and verify the `_delta_log/` reflects both transactions
- Read the table back; confirm row count equals 100 original emails + 2 appended rows

**Done when:** A Batch job successfully writes, Z-orders, appends, and reads a Delta table on S3; `_delta_log/` contains at least 3 entries (initial write, Z-order optimize, append).

---

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

### Task 4 — Structured Streaming `foreachBatch` with S3 Checkpoint

**Goal:** Process a Delta entity source in controlled micro-batches with fault-tolerant checkpointing, mirroring the Job 2 streaming pattern.

**Prerequisite:** Task 1 and Task 3 complete — `entities/` Delta table exists on S3 (written in a prior step from `incoming_lawyers.json`).

**Setup:** As a Job 1 simulation, read `incoming_lawyers.json` (10 records), apply name normalization (lowercase, strip `Dr.`/`Ms.` prefixes), assign a UUID `entity_id`, and write to `s3://<bucket>/mdm/spark/2026_04_02_1/entities/` as Delta, repartitioned to 5 files (2 entities per file, `micro_batch_size=2`). This gives the streaming source 5 trigger files.

**Steps:**
- Implement `job2_candidate_search.py` in the package: read the `entities/` Delta table as a stream with `maxFilesPerTrigger=1` and `availableNow=True`
- In `foreachBatch`, log the micro-batch number and the `entity_id` + `full_name_normalized` values for that batch; write the micro-batch to `s3://<bucket>/mdm/spark/2026_04_02_1/candidates_raw/` in Delta `append` mode
- Set `checkpointLocation` to `s3://<bucket>/mdm/spark/2026_04_02_1/checkpoints/job2/`
- Run end-to-end; confirm 5 micro-batches processed, 10 total rows in `candidates_raw/`
- Simulate a mid-run failure: raise an exception inside `foreachBatch` when `microbatch_id == 2`
- Retry; confirm CloudWatch logs show micro-batches 0 and 1 are skipped, processing resumes from micro-batch 2; confirm `candidates_raw/` has no duplicate entity rows

**Done when:** Retry resumes from micro-batch 2; final `candidates_raw/` row count is exactly 10 with no duplicates.

---

### Task 5 — Multi-job Step Functions Pipeline with `ContainerOverrides`

**Goal:** Chain Job 1 and Job 2 in a Step Functions state machine, passing `batch_id` and `micro_batch_size` through execution input, mirroring the production 4-job pipeline structure.

**Steps:**
- Define a state machine with 2 sequential `batch:submitJob.sync` states: `Job1_IngestNormalize` → `Job2_CandidateSearch`
- Execution input: `{"batch_id": "2026_04_02_1", "micro_batch_size": "2"}`
- Pass `BATCH_ID` to both jobs and `MICRO_BATCH_SIZE` to Job 2 via `ContainerOverrides.Environment`
- Job 1 (`job1_ingest_normalize`): reads `incoming_lawyers.json` from SSM-fetched bucket, normalizes names, writes `entities/` Delta with 5 partitions (repartitioned by `micro_batch_size=2`); logs `batch_id` and entity count
- Job 2 (`job2_candidate_search`): runs the streaming micro-batch loop from Task 4, reading `BATCH_ID` and `MICRO_BATCH_SIZE` from env; logs each micro-batch number
- Add `Retry` (2 attempts, 30s interval, backoff 2.0) to each state
- Add `Catch → PipelineFailed` (`Type: Fail`) terminal on both states
- Execute the full pipeline; verify in CloudWatch that Job 1 logged `batch_id = 2026_04_02_1` and entity count 10, and Job 2 processed 5 micro-batches of 2 entities each
- Simulate a Job 1 failure (env var missing); confirm the state machine reaches `PipelineFailed` without starting Job 2

**Done when:** End-to-end execution produces 10 rows in `candidates_raw/` scoped under `2026_04_02_1/`; failure path confirmed.

---

### Task 6 — Jaro-Winkler UDF + Cross-join (Phase 2 Candidate Search)

**Goal:** Implement the full Job 2 candidate search — exact blocking (Phase 1) and fuzzy name fallback (Phase 2) — against the real synthetic data.

**Prerequisite:** Task 1 complete — `cluster_names/`, `cluster_emails/`, `cluster_linkedin/`, `cluster_phones/` Delta tables written from `existing_clusters.json`.

**Steps:**
- Add `jellyfish` to the Dockerfile
- Define `jaro_winkler_udf` as `@udf(returnType=FloatType())` with null handling (return `0.0` for either null input)
- In `foreachBatch`, implement Phase 1: inner join each micro-batch's entities against `cluster_emails/`, `cluster_linkedin/`, `cluster_phones/`; union results; `dropDuplicates(["entity_id", "master_id"])`
- Implement Phase 2: `left_anti` join against Phase 1 results to find unmatched entities; `crossJoin` against `cluster_names/`; apply UDF; filter `name_sim > 0.5`; keep top 10 per entity via `row_number().over(Window.partitionBy("entity_id").orderBy(col("name_sim").desc()))`
- Write `all_candidates` (Phase 1 ∪ Phase 2) to `candidates/` and `no_candidates` (left-anti of micro-batch against all_candidates) to `no_candidates/`, both Delta `append`
- Expected results across all 5 micro-batches:
  - `candidates/`: 8 rows — (L001→C001), (L002→C002), (L003→C003), (L004→C004), (L005→C005), (L006→C006), (L009→C007), (L010→C008)
  - `no_candidates/`: 2 rows — L007, L008
- Log per micro-batch: how many entities went through Phase 1 vs Phase 2

**Done when:** `candidates/` contains exactly 8 `(entity_id, master_id)` pairs and `no_candidates/` contains exactly 2 `entity_id` values, with no duplicates across retries.

---

### Task 7 — JDBC Read/Write to RDS from Batch (Optional — requires VPC setup)

**Goal:** Replace the JSON file reads with real JDBC reads from PostgreSQL, completing the substitution of synthetic data with a real database source.

**Prerequisite:** Tasks 1–6 complete. This task replaces `spark.read.json(incoming_lawyers_path)` and `spark.read.json(existing_clusters_path)` with JDBC reads, keeping all downstream Delta writes and streaming logic identical.

**Steps:**
- Create a free-tier RDS PostgreSQL instance in the same VPC as the Batch compute environment (private subnet, no public access)
- Create the tables and load the synthetic data into PostgreSQL:
  - `cleansed."Lawyer"` — schema matches `incoming_lawyers.json` columns; insert the 10 records with `batch_id = '2026_04_02_1'`
  - `mdm.cluster` — columns `master_id`, `entity_type`, `searching_keys` (JSONB); insert all 100 cluster records
- Add the PostgreSQL JDBC driver JAR to the Docker image; configure `spark.jars`
- Update `/mdm/pg_url` in SSM with the real JDBC connection string
- In `job1_ingest_normalize.py`, replace the JSON read with:
  ```python
  spark.read.format("jdbc").option("url", pg_url) \
      .option("dbtable", f"(SELECT * FROM cleansed.\"Lawyer\" WHERE batch_id = '{batch_id}') AS batch") \
      .option("driver", "org.postgresql.Driver").load()
  ```
  and similarly for `mdm.cluster`
- Confirm security group rules allow Batch → RDS on port 5432
- All downstream tasks (Delta writes, streaming, candidate search) run unchanged

**Done when:** Job 1 reads 10 lawyer rows and 100 cluster rows from RDS via JDBC; all subsequent jobs produce the same `candidates/` and `no_candidates/` output as in Tasks 4–6.
