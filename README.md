
# Azure and Databricks End-to-End Project (Spotify Dataset)

## Architecture Diagram
<img width="1536" height="1024" alt="Architecture Diagram" src="https://github.com/user-attachments/assets/8a02f354-1aff-45ea-88fa-f8cfcdd82426" />


## Overview

A production-grade, end-to-end Azure data engineering pipeline built on a pseudo Spotify dataset. This project implements a full medallion architecture (Bronze → Silver → Gold) with incremental data loading, slowly changing dimensions, metadata-driven pipelines, data quality checks, and CI/CD deployment using Databricks Asset Bundles.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Azure SQL Database | Source data store |
| Azure Data Factory (ADF) | Data ingestion and orchestration |
| Azure Data Lake Gen2 | Cloud storage (Bronze, Silver, Gold) |
| Azure Databricks | Data transformation and processing |
| Apache Spark (PySpark) | Distributed data processing |
| Delta Lake | Open table format for reliable data storage |
| Unity Catalog | Centralised data governance and catalogue |
| Delta Live Tables (Lakeflow) | Declarative pipelines for Gold layer |
| Jinja2 | Metadata-driven dynamic SQL generation |
| Logic Apps | Pipeline failure email alerting |
| GitHub | Version control and CI/CD |
| Databricks Asset Bundles | Environment deployment (dev → prod) |

---

## Architecture

The pipeline follows the **medallion architecture** pattern:

```
Azure SQL Database
        ↓
Azure Data Factory (incremental_loop pipeline)
        ↓
Bronze Layer  →  Raw parquet files in Data Lake
        ↓
Silver Layer  →  Cleaned Delta tables in Databricks
        ↓
Gold Layer    →  SCD Type 2 dimensions via Delta Live Tables
        ↓
SQL Warehouse →  BI tools (Power BI, Tableau)
```

---

## Project Structure

```
spotify_dab/
├── src/
│   ├── silver/
│   │   └── silver_dimensions.py       ← Batch processing for all 5 tables
│   ├── gold/
│   │   └── dlt/
│   │       └── transformations/
│   │           ├── dim_user.py        ← SCD Type 2
│   │           ├── dim_track.py       ← SCD Type 2
│   │           ├── dim_date.py        ← SCD Type 2
│   │           └── fact_stream.py     ← SCD Type 1 (upsert)
│   ├── ginja/
│   │   └── ginja_notebook.py          ← Jinja metadata-driven SQL views
│   └── utils/
│       └── transformations.py         ← Reusable Python utility class
├── databricks.yml                     ← Asset Bundle config (dev + prod)
└── README.md
```
<img width="336" height="542" alt="Screenshot 2026-05-29 150919" src="https://github.com/user-attachments/assets/5c078a38-de1f-4366-bbd9-ec1596ff53f9" />

---

## Part 1 — Source Layer

**Azure SQL Database** contains 5 tables representing a Spotify-like music streaming service:

- `DimUser` — user profiles (username, country, subscription type)
- `DimTrack` — track details (track name, duration, album)
- `DimArtist` — artist details (artist name, genre, country)
- `DimDate` — date dimension
- `FactStream` — streaming events (user, track, device, timestamp)
<img width="1514" height="495" alt="image" src="https://github.com/user-attachments/assets/8b842a11-8f0d-4822-8734-980d5a533e85" />


---

## Part 2 — Bronze Layer (Azure Data Factory)

**Key features:**

**Incremental loading** — ADF only pulls new records on each run using a CDC watermark stored in a JSON file per table. The watermark stores the last `MAX(updated_at)` timestamp.

**Three scenarios handled in one dynamic pipeline:**
- Initial load — CDC starts at `1900-01-01`, pulls everything
- Incremental load — only new records since last run
- Backfill — pass a `from_date` parameter to reload from any past date

**Loop pipeline** — a single `incremental_loop` pipeline processes all 5 tables dynamically using a `ForEach` activity and a `loop_input.json` config array. To add a new table, just add one dictionary entry to the array.

**Logic Apps alerting** — on pipeline failure, ADF sends an HTTP POST to a Logic Apps endpoint which triggers an automatic email with the pipeline name and run ID.

**Git integration** — all ADF pipelines are version controlled in GitHub. The `main` branch holds source JSON. The `adf_publish` branch holds ARM templates for DevOps deployment.

<img width="1104" height="363" alt="Screenshot 2026-05-29 150802" src="https://github.com/user-attachments/assets/c987e180-848f-49a5-a791-18532f69993d" /> 
<img width="1162" height="493" alt="image" src="https://github.com/user-attachments/assets/cb3e55c3-4b33-4ceb-b8e5-85e850b25b82" />
<img width="1176" height="636" alt="Screenshot 2026-05-29 150848" src="https://github.com/user-attachments/assets/6a213d43-8233-4cae-a229-e1f757ee8c0f" />
<img width="650" height="402" alt="email" src="https://github.com/user-attachments/assets/72374a31-47f5-4767-961c-ba99d37c1aa9" />

---

## Part 3 — Silver Layer (Azure Databricks)

All 5 tables processed in **batch mode** using PySpark. Data read from Bronze parquet files and written as Delta tables to Silver container in Unity Catalog (`spotify_cata.silver`).

**Transformations applied:**

| Table | Transformation |
|-------|---------------|
| DimUser | Convert `user_name` to uppercase |
| DimTrack | Add `duration_flag` (low/medium/high), replace hyphens in `track_name` with spaces |
| DimArtist | Drop duplicates on `artist_id` |
| DimDate | Drop `_rescued_data`, load as is |
| FactStream | Drop `_rescued_data`, load as is |

**Reusable utility class** — a `Reusable` class in `utils/transformations.py` provides reusable methods like `dropColumns()` that can be imported across all notebooks.

**Jinja metadata-driven SQL views** — instead of writing SQL joins from scratch for every business request, a Jinja2 template dynamically generates SQL queries from a parameters array. Adding a new join requires only adding a dictionary entry to the array — no code changes.
<img width="1578" height="832" alt="Screenshot 2026-05-29 161402" src="https://github.com/user-attachments/assets/da683f78-7070-461a-aa82-5eda2fdea1d2" />


```python
parameters = [
    {
        "table": "spotify_cata.silver.FactStream",
        "alias": "fact_stream",
        "columns": ["fact_stream.stream_id", "fact_stream.listen_duration"],
        "condition": None
    },
    {
        "table": "spotify_cata.silver.DimUser",
        "alias": "dim_user",
        "columns": ["dim_user.user_name"],
        "condition": "fact_stream.user_id = dim_user.user_id"
    }
]
```

---

## Part 4 — Gold Layer (Delta Live Tables / Lakeflow)

The Gold layer uses **Databricks Delta Live Tables** (declarative pipelines) to implement Slowly Changing Dimensions.

**SCD Type 2 (versioning)** applied to dimension tables:
- When a value changes, the old record is expired (end date populated)
- A new record is inserted as the current version
- Full history is preserved for auditing and analytics

**SCD Type 1 (upsert)** applied to FactStream:
- Each transaction is already a historical record
- No need to store history — just overwrite with latest values

**Data quality expectations** on DimUser:
- Rule: `user_id IS NOT NULL`
- Action: `DROP` — records failing the check are automatically dropped
- Results visible in the pipeline DAG with pass/fail metrics

```python
dlt.apply_changes(
    target="dim_user",
    source="dim_user_staging",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)


```
<img width="849" height="618" alt="Screenshot 2026-05-29 161256" src="https://github.com/user-attachments/assets/8590cd42-37af-49be-a5a9-a2505ca8d422" />

---

## Part 5 — Databricks Asset Bundles

All Databricks code is packaged and deployed using **Databricks Asset Bundles (DAB)**.

```bash
# Validate the bundle
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

The `databricks.yml` config defines both environments with separate host URLs and permissions. In a real organisation, a DevOps engineer picks the bundle from dev and deploys to QA and prod — the data engineer only creates and deploys to dev.

---

## How to Run

### Prerequisites
- Azure subscription
- Azure SQL Database with source data loaded (`source_scripts/spotify_initial_load.sql`)
- Azure Data Lake Gen2 with Bronze, Silver, Gold containers
- Azure Databricks workspace with Unity Catalog enabled
- Azure Data Factory connected to GitHub repo

### Steps

1. **Load source data** — run `source_scripts/spotify_initial_load.sql` in Azure SQL Query Editor
2. **Run ADF pipeline** — trigger `incremental_loop` in Azure Data Factory
3. **Run Silver notebook** — run `src/silver/silver_dimensions.py` in Databricks
4. **Run Gold pipeline** — trigger the Lakeflow DLT pipeline in Databricks Jobs → Pipelines
5. **Query results** — connect to SQL Warehouse and query `spotify_cata.gold.*`

### Incremental test
```sql
-- Check SCD Type 2 expired records
SELECT * FROM spotify_cata.gold.dim_user WHERE __END_AT IS NOT NULL

-- Check both versions of a changed record
SELECT * FROM spotify_cata.gold.dim_track WHERE track_id IN (46, 5)
```

---

## Key Concepts Demonstrated

- **Medallion architecture** — Bronze → Silver → Gold data quality layers
- **Incremental loading** — CDC watermark pattern, never bulk loading
- **Backfill capability** — reload any date range without rebuilding everything
- **SCD Type 2** — full history tracking for dimension tables
- **Metadata-driven pipelines** — Jinja templating for dynamic SQL, loop pipelines for dynamic table processing
- **Data quality** — DLT expectations with drop/warn/fail modes
- **Idempotency** — pipelines can be re-run safely without creating duplicates
- **Modular code** — reusable Python utility classes, parameterised pipelines
- **CI/CD** — GitHub branching strategy, ADF ARM templates, Databricks Asset Bundles
- **Monitoring** — Logic Apps email alerting on pipeline failure

---

## Author

**Dhruvkumar Patel**
