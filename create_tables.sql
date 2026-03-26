-- ================================================================
-- COVID-19 ETL Pipeline — PostgreSQL Schema
-- Author  : Mohani Gupta | mohanigupta279@gmail.com
-- ================================================================

CREATE DATABASE IF NOT EXISTS covid_db;

-- ── Country Snapshot (refreshed daily) ───────────────────────
CREATE TABLE IF NOT EXISTS covid_countries (
    id                      SERIAL PRIMARY KEY,
    country                 VARCHAR(100) NOT NULL,
    continent               VARCHAR(50),
    cases                   BIGINT   DEFAULT 0,
    today_cases             INT      DEFAULT 0,
    deaths                  BIGINT   DEFAULT 0,
    today_deaths            INT      DEFAULT 0,
    recovered               BIGINT   DEFAULT 0,
    active                  BIGINT   DEFAULT 0,
    critical                INT      DEFAULT 0,
    cases_per_million       NUMERIC(12,2),
    deaths_per_million      NUMERIC(12,2),
    tests                   BIGINT   DEFAULT 0,
    tests_per_million       NUMERIC(12,2),
    population              BIGINT   DEFAULT 0,
    case_fatality_rate_pct  NUMERIC(6,3),
    recovery_rate_pct       NUMERIC(6,3),
    test_positivity_rate_pct NUMERIC(6,3),
    updated_at              TIMESTAMPTZ,
    loaded_at               TIMESTAMPTZ DEFAULT NOW()
);

-- ── Historical Time Series (upserted daily) ───────────────────
CREATE TABLE IF NOT EXISTS covid_historical (
    id               SERIAL PRIMARY KEY,
    date             DATE   NOT NULL,
    country          VARCHAR(100) DEFAULT 'Global',
    cases            BIGINT DEFAULT 0,
    deaths           BIGINT DEFAULT 0,
    recovered        BIGINT DEFAULT 0,
    daily_cases      INT    DEFAULT 0,
    daily_deaths     INT    DEFAULT 0,
    cases_7day_avg   NUMERIC(12,2),
    deaths_7day_avg  NUMERIC(12,2),
    cases_14day_avg  NUMERIC(12,2),
    growth_rate_pct  NUMERIC(8,3),
    loaded_at        TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_date_country UNIQUE (date, country)
);

-- ── Pipeline Run Log ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    run_timestamp   TIMESTAMPTZ DEFAULT NOW(),
    status          VARCHAR(20)  DEFAULT 'success',
    countries_loaded INT,
    historical_rows  INT,
    duration_secs    NUMERIC(8,2),
    notes           TEXT
);

-- ── Indexes ───────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_hist_date    ON covid_historical(date);
CREATE INDEX IF NOT EXISTS idx_hist_country ON covid_historical(country);
CREATE INDEX IF NOT EXISTS idx_ctry_name    ON covid_countries(country);
CREATE INDEX IF NOT EXISTS idx_ctry_cont    ON covid_countries(continent);

SELECT tablename FROM pg_catalog.pg_tables
WHERE schemaname = 'public' ORDER BY tablename;
