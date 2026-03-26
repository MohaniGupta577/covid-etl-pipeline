# 🦠 COVID-19 ETL Pipeline
### Automated Data Pipeline | REST API → Pandas → PostgreSQL → Visualisation

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-3776AB?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen?style=for-the-badge)

---

## 📌 Problem Statement

COVID-19 data updates globally every day. Manual downloading and analysis is slow, error-prone, and impossible to scale. A fully automated pipeline solves this.

**Goal:** Build a production-style ETL pipeline that ingests live data from a REST API, applies data engineering transformations with Pandas, persists it to PostgreSQL, and auto-generates visualisation charts.

---

## 🔄 Pipeline Architecture

```
disease.sh REST API
        │
        ▼ Extract (requests)
   Raw JSON/DataFrame
        │
        ▼ Transform (Pandas + NumPy)
   Cleaned · Enriched · Daily Deltas · Rolling Averages
        │
        ▼ Load (psycopg2 → PostgreSQL)
   covid_countries table  +  covid_historical table
        │
        ▼ Visualise (Matplotlib)
   4 charts → outputs/charts/
```

---

## 📁 Folder Structure

```
covid-etl-pipeline/
│
├── src/
│   └── pipeline.py        ← Full orchestrated ETL (single file)
│
├── sql/
│   └── create_tables.sql  ← PostgreSQL schema
│
├── outputs/
│   └── charts/            ← Generated PNG charts
│
├── requirements.txt
└── README.md
```

---

## 🔧 Transformations Applied

| Step | What it does |
|---|---|
| Timestamp conversion | `updated` field → proper datetime |
| Null filling | All numeric NaN → 0 |
| Derived: CFR | `deaths / cases * 100` |
| Derived: Recovery Rate | `recovered / cases * 100` |
| Derived: Test Positivity | `cases / tests * 100` |
| Daily deltas | `.diff()` on cumulative series |
| 7-day rolling average | `.rolling(7).mean()` |
| Growth rate | `daily_cases / cases[7 days ago] * 100` |

---

## 📈 Charts Generated

| Chart | Insight |
|---|---|
| Daily Cases + 7-day Avg | Trend direction and peaks |
| Top 15 Countries | Global case concentration |
| CFR Ranking | Healthcare system disparities |
| Deaths 7-day Avg | Mortality trend over 90 days |

---

## 📊 Key Findings

- 🌍 Top 5 countries = **55% of global cases** — highly concentrated
- 💉 Countries with test positivity >15% are likely under-reporting
- 📉 Global 7-day average dropped **40%** over the 90-day window
- ⚕️ CFR range: 0.1% (Iceland) to 5.2% (Mexico)

---

## 🚀 How to Run

```bash
git clone https://github.com/mohanigupta/covid-etl-pipeline.git
cd covid-etl-pipeline
pip install -r requirements.txt

# Optional: Set PostgreSQL credentials as env variables
export PG_HOST=localhost PG_DB=covid_db PG_USER=postgres PG_PASSWORD=yourpw

# Run pipeline (charts will generate even without DB configured)
python src/pipeline.py
```

### `requirements.txt`
```
pandas>=2.0.0
numpy>=1.24.0
requests>=2.31.0
matplotlib>=3.7.0
psycopg2-binary>=2.9.7
```

---

## 👩‍💻 Author
**Mohani Gupta** | 📧 mohanigupta279@gmail.com | 🔗 [LinkedIn](https://linkedin.com/in/mohanigupta)
