# ================================================================
# COVID-19 ETL Pipeline — pipeline.py (Orchestrator)
# Author  : Mohani Gupta | mohanigupta279@gmail.com
# Flow    : API → Pandas Transform → PostgreSQL → Visualise
# ================================================================

import logging
import os
import requests
import pickle

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s"
)
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────────
API_BASE = "https://disease.sh/v3/covid-19"

DB_CONFIG = {
    "host"    : os.getenv("PG_HOST",     "localhost"),
    "database": os.getenv("PG_DB",       "covid_db"),
    "user"    : os.getenv("PG_USER",     "postgres"),
    "password": os.getenv("PG_PASSWORD", "your_password"),
    "port"    : int(os.getenv("PG_PORT", 5432)),
}

os.makedirs("outputs/charts", exist_ok=True)


# ════════════════════════════════════════════════════════════════
# EXTRACT
# ════════════════════════════════════════════════════════════════

def extract_countries() -> pd.DataFrame:
    """Fetch per-country COVID-19 summary."""
    log.info("Fetching country-level data from API...")
    r = requests.get(f"{API_BASE}/countries", timeout=20)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    log.info(f"  → {len(df)} countries extracted")
    return df


def extract_historical(days: int = 90) -> pd.DataFrame:
    """Fetch global historical time-series (last N days)."""
    log.info(f"Fetching global historical data (last {days} days)...")
    r = requests.get(f"{API_BASE}/historical/all?lastdays={days}", timeout=20)
    r.raise_for_status()
    raw = r.json()

    records = [
        {
            "date"     : pd.to_datetime(date),
            "cases"    : raw["cases"].get(date, 0),
            "deaths"   : raw["deaths"].get(date, 0),
            "recovered": raw["recovered"].get(date, 0),
        }
        for date in raw["cases"]
    ]
    df = pd.DataFrame(records).sort_values("date").reset_index(drop=True)
    log.info(f"  → {len(df)} days extracted")
    return df


# ════════════════════════════════════════════════════════════════
# TRANSFORM
# ════════════════════════════════════════════════════════════════

def transform_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Clean, select, and enrich country-level data."""
    keep = [
        "country", "continent", "cases", "todayCases",
        "deaths", "todayDeaths", "recovered", "active",
        "critical", "casesPerOneMillion", "deathsPerOneMillion",
        "tests", "testsPerOneMillion", "population", "updated",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Convert timestamp
    df["updated"] = pd.to_datetime(df["updated"], unit="ms")

    # Fill numeric NaN
    num_cols = df.select_dtypes("number").columns
    df[num_cols] = df[num_cols].fillna(0)

    # Derived metrics
    df["case_fatality_rate_pct"] = np.where(
        df["cases"] > 0,
        (df["deaths"] / df["cases"] * 100).round(3), 0
    )
    df["recovery_rate_pct"] = np.where(
        df["cases"] > 0,
        (df["recovered"] / df["cases"] * 100).round(3), 0
    )
    df["test_positivity_rate_pct"] = np.where(
        df["tests"] > 0,
        (df["cases"] / df["tests"] * 100).round(3), 0
    )

    df = df.sort_values("cases", ascending=False).reset_index(drop=True)
    log.info(f"  → {len(df)} countries after transform")
    return df


def transform_historical(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich historical data with daily deltas and rolling averages."""
    df = df.sort_values("date").reset_index(drop=True)

    df["daily_cases"]      = df["cases"].diff().clip(lower=0)
    df["daily_deaths"]     = df["deaths"].diff().clip(lower=0)
    df["cases_7day_avg"]   = df["daily_cases"].rolling(7,  min_periods=1).mean().round(0)
    df["deaths_7day_avg"]  = df["daily_deaths"].rolling(7, min_periods=1).mean().round(0)
    df["cases_14day_avg"]  = df["daily_cases"].rolling(14, min_periods=1).mean().round(0)
    df["growth_rate_pct"]  = (
        df["daily_cases"] / df["cases"].shift(7).replace(0, np.nan) * 100
    ).round(3)

    log.info(f"  → {len(df)} historical records after transform")
    return df


# ════════════════════════════════════════════════════════════════
# LOAD
# ════════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def load_countries(df: pd.DataFrame) -> None:
    """Truncate & reload country snapshot."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("TRUNCATE TABLE covid_countries RESTART IDENTITY;")
    rows  = [tuple(row) for row in df.itertuples(index=False)]
    cols  = ", ".join(df.columns)
    ph    = ", ".join(["%s"] * len(df.columns))
    psycopg2.extras.execute_batch(
        cur, f"INSERT INTO covid_countries ({cols}) VALUES ({ph})", rows, page_size=500
    )
    conn.commit(); cur.close(); conn.close()
    log.info(f"  → Loaded {len(df)} country rows into PostgreSQL")


def load_historical(df: pd.DataFrame) -> None:
    """Upsert historical records."""
    conn = get_conn()
    cur  = conn.cursor()
    rows  = [tuple(row) for row in df.itertuples(index=False)]
    cols  = ", ".join(df.columns)
    ph    = ", ".join(["%s"] * len(df.columns))
    psycopg2.extras.execute_batch(
        cur,
        f"""INSERT INTO covid_historical ({cols}) VALUES ({ph})
            ON CONFLICT (date) DO UPDATE
            SET cases    = EXCLUDED.cases,
                deaths   = EXCLUDED.deaths,
                daily_cases   = EXCLUDED.daily_cases,
                daily_deaths  = EXCLUDED.daily_deaths,
                cases_7day_avg = EXCLUDED.cases_7day_avg""",
        rows, page_size=500
    )
    conn.commit(); cur.close(); conn.close()
    log.info(f"  → Loaded {len(df)} historical rows into PostgreSQL")


# ════════════════════════════════════════════════════════════════
# VISUALISE
# ════════════════════════════════════════════════════════════════

def visualise(countries: pd.DataFrame, hist: pd.DataFrame) -> None:
    """Generate 4 charts and save to outputs/charts/."""
    log.info("Generating charts...")

    # 1. Global Daily Cases with 7-day average
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.bar(hist["date"], hist["daily_cases"],
           color="#4f8ef7", alpha=0.4, label="Daily Cases")
    ax.plot(hist["date"], hist["cases_7day_avg"],
            color="#ef4444", linewidth=2.2, label="7-Day Average")
    ax.set_title("Global Daily COVID-19 Cases",
                 fontsize=15, fontweight="bold")
    ax.set_xlabel("Date"); ax.set_ylabel("Cases")
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x/1e6:.1f}M")
    )
    ax.legend()
    plt.tight_layout()
    plt.savefig("outputs/charts/01_daily_cases.png", dpi=150)
    plt.close()

    # 2. Top 15 Countries by Total Cases
    top15 = countries.head(15).sort_values("cases")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top15["country"], top15["cases"] / 1e6,
            color="#4f8ef7", alpha=0.85)
    ax.set_title("Top 15 Countries by Total Cases",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Total Cases (Millions)")
    plt.tight_layout()
    plt.savefig("outputs/charts/02_top15_countries.png", dpi=150)
    plt.close()

    # 3. Case Fatality Rate — Top 20 Countries
    cfr = (
        countries[countries["cases"] > 100_000]
        .nlargest(20, "case_fatality_rate_pct")
        .sort_values("case_fatality_rate_pct")
    )
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(cfr["country"], cfr["case_fatality_rate_pct"],
            color="#ef4444", alpha=0.8)
    ax.set_title("Case Fatality Rate — Top 20 Countries",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("CFR (%)")
    plt.tight_layout()
    plt.savefig("outputs/charts/03_cfr_ranking.png", dpi=150)
    plt.close()

    # 4. Deaths 7-day average trend
    fig, ax = plt.subplots(figsize=(14, 4))
    ax.fill_between(hist["date"], hist["deaths_7day_avg"],
                    color="#f59e0b", alpha=0.6, label="7-Day Avg Deaths")
    ax.plot(hist["date"], hist["deaths_7day_avg"],
            color="#f59e0b", linewidth=1.5)
    ax.set_title("Global Daily Deaths (7-Day Rolling Average)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Date"); ax.set_ylabel("Deaths")
    plt.tight_layout()
    plt.savefig("outputs/charts/04_deaths_trend.png", dpi=150)
    plt.close()

    log.info("  → 4 charts saved to outputs/charts/")


# ════════════════════════════════════════════════════════════════
# ORCHESTRATE
# ════════════════════════════════════════════════════════════════

def run_pipeline():
    log.info("=" * 55)
    log.info("  COVID-19 ETL PIPELINE  —  Mohani Gupta")
    log.info("=" * 55)

    log.info("\n📥 STEP 1: EXTRACT")
    countries_raw = extract_countries()
    historical_raw = extract_historical(days=90)

    log.info("\n🔧 STEP 2: TRANSFORM")
    countries_clean = transform_countries(countries_raw)
    historical_clean = transform_historical(historical_raw)

    log.info("\n💾 STEP 3: LOAD")
    try:
        load_countries(countries_clean)
        load_historical(historical_clean)
    except Exception as e:
        log.warning(f"DB load skipped (configure DB_CONFIG): {e}")

    log.info("\n📊 STEP 4: VISUALISE")
    visualise(countries_clean, historical_clean)

    log.info("\n✅ Pipeline completed successfully!")


if __name__ == "__main__":
    run_pipeline()
