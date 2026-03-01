# NYC Taxi Pipeline — Bruin + MotherDuck

> **Built with AI Agent assistance** — This project was developed interactively with an AI agent (Claude), fixing errors step by step, configuring connections, and deploying to the cloud.

## 🗺️ Project Overview

An end-to-end data pipeline that ingests NYC Taxi trip data, transforms it through staging layers, and produces aggregated reports — fully automated in the cloud.

```
NYC Taxi Data (parquet) 
    → ingestion layer (MotherDuck)
    → staging layer (cleaned)
    → reports layer (aggregated)
```

**Stack:**
- **Bruin** — pipeline orchestration & scheduling (like Airflow, but simpler)
- **MotherDuck** — cloud DuckDB database (like BigQuery, but lighter)
- **Bruin Cloud** — managed scheduler that runs the pipeline automatically every month
- **GitHub** — source of truth for pipeline code

---

## 📁 Project Structure

```
my-taxi-pipeline/
├── pipeline/
│   ├── pipeline.yml                          # Pipeline config (schedule, connections)
│   └── assets/
│       ├── ingestion/
│       │   ├── trips.py                      # Python asset: fetches parquet files
│       │   ├── payment_lookup.asset.yml      # Seed asset: payment type lookup
│       │   ├── payment_lookup.csv            # Seed data
│       │   └── requirements.txt
│       ├── staging/
│       │   └── trips.sql                     # DuckDB SQL: clean & deduplicate
│       └── reports/
│           └── trips_report.sql              # DuckDB SQL: aggregated report
├── .bruin.yml                                # Connection config (MotherDuck)
├── .gitignore
└── README.md
```

---

## ⚙️ Pipeline Configuration

**`pipeline.yml`**
```yaml
name: nyc-taxi-pipeline
schedule: monthly
start_date: "2025-01-01"
default_connections:
  duckdb: motherduck-prod
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

**`.bruin.yml`** (root level)
```yaml
default_environment: default
environments:
  default:
    connections:
      motherduck:
        - name: "motherduck-prod"
          token: "${MOTHERDUCK_TOKEN}"
```

---

## 🚀 Setup & Deployment

### Prerequisites
- [Bruin CLI](https://getbruin.com/docs) installed
- [MotherDuck](https://app.motherduck.com) account with a token
- [Bruin Cloud](https://getbruin.com) account connected to GitHub

---

### 1. Local Development

```bash
# Validate pipeline
bruin validate ./pipeline

# Run a single month (test)
bruin run ./pipeline/ --full-refresh --start-date 2025-01-01 --end-date 2025-01-31

# Run quality checks only
bruin run ./pipeline/ --only checks
```

---

### 2. MotherDuck Setup

1. Sign up at [app.motherduck.com](https://app.motherduck.com)
2. Get your token: **Settings → Tokens**
3. Test connection locally:
```python
import duckdb
con = duckdb.connect('md:?motherduck_token=YOUR_TOKEN')
print(con.execute('SELECT 1').fetchall())  # Should print [(1,)]
```
4. Add to root `.bruin.yml` under `motherduck.token`

---

### 3. Bruin Cloud Deployment

1. Push code to GitHub:
```bash
git add .
git commit -m "your message"
git push origin master
```

2. Go to [getbruin.com](https://getbruin.com) → **Settings → Projects → Add New Project**
3. Connect your GitHub repo (`try_bruin_motherduck`, branch: `master`)
4. Go to **Settings → Connections** → add `motherduck-prod` with your token
5. Go to **Pipelines** → enable `nyc-taxi-pipeline`
6. Click **+ New Run** to trigger a manual run

---


## 📊 Data Flow

| Layer | Table | Description |
|---|---|---|
| Ingestion | `ingestion.trips` | Raw parquet data from NYC TLC |
| Ingestion | `ingestion.payment_lookup` | Payment type seed data |
| Staging | `staging.trips` | Cleaned, deduplicated trips |
| Reports | `reports.trips_report` | Aggregated by date, taxi type, payment |

**Report columns:** `pickup_date`, `taxi_type`, `payment_type_name`, `trip_count`, `total_passengers`, `total_distance_miles`, `total_fare_amount`, `avg_fare_amount`, `avg_trip_distance_miles`

---

## 🔑 Key Commands Reference

| Command | Purpose |
|---|---|
| `bruin validate ./pipeline` | Check syntax & dependencies |
| `bruin run ./pipeline/ --full-refresh` | Run all assets from scratch |
| `bruin run ./pipeline/ --start-date X --end-date Y` | Run for specific interval |
| `bruin connections test --name motherduck-prod` | Test connection |
| `bruin lineage ./pipeline` | View asset dependencies |

---

## 💡 Architecture Decisions

**Why MotherDuck over BigQuery?**
- Same DuckDB SQL dialect — no syntax migration needed
- Simpler setup, no GCP project required
- Cost-effective for small/medium workloads

**Why Bruin over Airflow?**
- No Docker/Kubernetes setup required
- Built-in data quality checks
- Assets defined alongside SQL/Python code
- Bruin Cloud = managed, no infra to maintain

---

## 📚 Resources

- [Bruin Documentation](https://getbruin.com/docs)
- [MotherDuck Documentation](https://motherduck.com/docs)
- [NYC TLC Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
