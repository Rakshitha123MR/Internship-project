CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS vitals_history (
    event_time TIMESTAMPTZ NOT NULL,
    patient_id TEXT NOT NULL,
    vital_type TEXT NOT NULL,
    vital_value TEXT NOT NULL,
    arrival_time TIMESTAMPTZ DEFAULT NOW(),
    quality_status TEXT DEFAULT 'VALID'
);

SELECT create_hypertable('vitals_history', 'event_time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS dq_events (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ DEFAULT NOW(),
    patient_id TEXT NOT NULL,
    vital_type TEXT NOT NULL,
    vital_value TEXT,
    quality_reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_audit (
    id BIGSERIAL PRIMARY KEY,
    alert_time TIMESTAMPTZ DEFAULT NOW(),
    patient_id TEXT NOT NULL,
    patient_name TEXT,
    bed_number TEXT,
    physician TEXT,
    news2_score INT,
    risk_category TEXT,
    alert_reason TEXT,
    alert_message TEXT
);

CREATE TABLE IF NOT EXISTS hourly_news2_report (
    id BIGSERIAL PRIMARY KEY,
    report_time TIMESTAMPTZ DEFAULT NOW(),
    total_patients INT,
    low_risk_count INT,
    medium_risk_count INT,
    high_risk_count INT,
    critical_risk_count INT,
    total_alerts INT,
    total_dq_events INT
);
