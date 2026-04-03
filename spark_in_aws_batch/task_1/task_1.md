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

**Done when:** 
A Batch job successfully writes, Z-orders and reads a Delta table on S3; `_delta_log/` 
contains at least 2 entries (initial write, Z-order optimize).

