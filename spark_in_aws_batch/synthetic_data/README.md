# Synthetic Data — MDM Spark Pipeline Sandbox

Both files are **JSONL** (one JSON object per line). Load with `spark.read.json(path)` — no `multiLine` option needed.

---

## incoming_lawyers.json

Substitutes `cleansed.Lawyer` filtered by `batch_id = "2026_04_02_1"`.  
Schema mirrors what JDBC returns from PostgreSQL: all columns present, nulls where the source does not supply a field.

| id   | source  | name               | match path           | expected outcome         |
|------|---------|--------------------|----------------------|--------------------------|
| L001 | pirical | John Smith         | exact email          | candidate → C001         |
| L002 | parser  | Sarah Johnson      | exact linkedin slug  | candidate → C002         |
| L003 | pirical | Michael Brown      | exact phone          | candidate → C003         |
| L004 | parser  | Emily Davies       | fuzzy name (Phase 2) | candidate → C004 ("emily davis", Jaro-Winkler ≈ 0.97) |
| L005 | pirical | Robert Wilson      | exact email + linkedin | candidate → C005       |
| L006 | parser  | Ms. Jennifer Lee   | fuzzy name (Phase 2) | candidate → C006 (after prefix strip: "jennifer lee") |
| L007 | pirical | David Martinez     | no match             | → no_candidates          |
| L008 | parser  | Lisa Thompson      | no match             | → no_candidates          |
| L009 | pirical | James Anderson     | exact phone (UK E.164) | candidate → C007       |
| L010 | parser  | Dr. Patricia White | fuzzy name (Phase 2) | candidate → C008 (after prefix strip: "patricia white") |

**Normalization edge cases exercised:**
- `pirical` source: name built from `first_name + last_name` (L001, L003, L005, L007, L009)
- `parser` source: name taken from `full_name` directly (L002, L004, L006, L008, L010)
- Prefix stripping: `Ms.` (L006), `Dr.` (L010)
- LinkedIn URL → slug extraction and hyphen removal: L001, L002, L005
- Phone E.164 normalization with country code: L001, L003, L007, L009 (L009 is UK)
- Practice areas as `·`-delimited string (pirical): L001, L003, L005, L007, L009
- Practice areas as JSON array (parser): L002, L004, L006, L008, L010
- Nulls in all identifier fields (L004, L006, L010) — forces Phase 2 fuzzy path

---

## existing_clusters.json

Substitutes `mdm.cluster` (full table read, no filter).  
100 records: C001–C008 are the matching clusters; C009–C100 are background noise.

**`searching_keys` is a JSON-encoded string** — this matches the raw string value that PostgreSQL JDBC returns for a JSONB column. Job 1 parses it with `from_json(col("searching_keys"), searching_keys_schema)`.

Schema per record:
```
master_id       string
entity_type     string   (always "Lawyer" here)
searching_keys  string   (JSON-encoded: emails[], linkedin_slugs[], phones[], full_names_normalized[])
```

| master_id | matched by          |
|-----------|---------------------|
| C001      | email (L001)        |
| C002      | linkedin slug (L002)|
| C003      | phone (L003)        |
| C004      | fuzzy name (L004)   |
| C005      | email (L005)        |
| C006      | fuzzy name (L006)   |
| C007      | phone (L009)        |
| C008      | fuzzy name (L010)   |
| C009–C100 | no match (background noise) |
