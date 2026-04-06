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