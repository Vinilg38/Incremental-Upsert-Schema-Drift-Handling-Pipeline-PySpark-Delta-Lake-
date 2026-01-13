📦 Delta Lake Incremental Upsert + Schema Drift + Late Arrival Handling

A PySpark + Delta Lake pipeline built on Databricks Free Edition

This project demonstrates a production-style ingestion pattern implemented entirely in batch mode, using Delta Lake features available in the Databricks Free Edition.

It covers:

✅ Incremental upserts (MERGE INTO)
✅ Schema drift detection (additive vs breaking changes)
✅ Late-arriving data handling using a watermark-like rule
✅ Bronze → Silver → Gold Lakehouse modeling
✅ Latency KPI calculation
✅ Quarantine tables for invalid or late rows

⸻

🚀 Architecture Overview (Batch Mode)

          Raw Batch Input (CSV / Parquet / API )
                  │
                  ▼
        ┌──────────────────────┐
        │    Bronze Layer       │
        │ (Raw + schema drift)  │
        └─────────────┬────────┘
                      │ mergeSchema=true
                      ▼
        ┌──────────────────────┐
        │    Silver Layer      │
        │ (incremental MERGE)  │
        └─────────────┬────────┘
                      │ dedupe + late fix + KPIs
                      ▼
        ┌──────────────────────┐
        │     Gold Layer       │
        │  aggregated metrics  │
        └──────────────────────┘

Additional pipelines:
	•	quarantine_schema → breaking schema drift
	•	quarantine_late → events older than watermark window

⸻

🧠 Key Features Explained

🔄 1. Incremental Upserts 

This pipeline uses:

MERGE INTO silver USING bronze

with:

WHEN MATCHED AND b.ingest_time > s.ingest_time THEN UPDATE

This ensures:
	•	Late-arriving updates supersede older versions
	•	No duplicate rows
	•	Bronze → Silver stays idempotent

Important:
This is NOT Databricks Change Data Feed (CDF)–based CDC.
Free Edition does not support CDF.

This is a merge-based incremental upsert pattern, commonly used when streaming is unavailable.

⸻

🔧 2. Schema Drift Handling

We detect whether the incoming batch is:

Additive drift (safe)
Example: new column source appears.

Action:
✔ Allow ingest with:

.option("mergeSchema", "true")

Breaking drift (unsafe)
Example: existing required column missing from batch.

Action:
🚫 Quarantine to quarantine_schema
✔ Prevent corruption of Silver and Gold

⸻

⏱️ 3. Late Arrival Handling (Watermark-like)

Since Free Edition does NOT support Structured Streaming watermarks, we simulate a batch-watermark:

datediff(reference_date, event_date) > 5 days

Rows older than this threshold go to:

quarantine_late

This avoids polluting final aggregates with extremely old events.

⸻

🏗️ 4. Lakehouse Layers

Layer	Purpose	Notes
Bronze	Raw ingestion + schema evolution	mergeSchema applied
Silver	Incremental upserts	MERGE INTO used
Gold	Aggregations & KPIs	latency, counts, sums


⸻

📊 Gold Layer Output

Sample result:

+--------+-------------+------------+-----------+
|user_id |total_events |total_amount|late_events|
+--------+-------------+------------+-----------+
|101     |1            |120.0       |0          |
|103     |1            |90.0        |0          |
|102     |1            |85.0        |1          |
+--------+-------------+------------+-----------+

User 102 is correctly tagged with a late event based on latency > 10 min.

⸻

▶️ How to Run
	1.	Create a new Notebook in Databricks Free Edition
	2.	Paste the provided code into separate cells
	3.	Run sequentially

⸻

📚 Concepts Demonstrated (Interview-Ready)
	•	Incremental ingestion with MERGE
	•	Idempotent upsert logic
	•	Schema drift prevention
	•	Late-arrival filtering without streaming
	•	Lakehouse architecture design
	•	Latency KPI derivation
	•	Data quality via quarantine tables

⸻

🔧 Requirements
	•	Databricks Free Edition
	•	Python + PySpark
	•	Delta Lake enabled workspace

⸻

📝 Notes on Free Edition Limitations

The following features do NOT work in Free Edition:

❌ Auto schema evolution via

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

❌ Structured Streaming (no true watermarking)

❌ Change Data Feed (actual CDC)

All logic implemented here is batch-mode safe and does not rely on enterprise-only features.

