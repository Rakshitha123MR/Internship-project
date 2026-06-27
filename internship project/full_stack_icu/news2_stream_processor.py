import argparse
import json
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, Producer


KAFKA_BOOTSTRAP = "localhost:29092"
RAW_TOPIC = "raw_vitals"
DQ_TOPIC = "dq_events"
ALERT_TOPIC = "alerts"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "icu_db",
    "user": "icu_user",
    "password": "icu_pass",
}

BOUNDS = {
    "heart_rate": (20, 250),
    "spo2": (50, 100),
    "resp_rate": (4, 60),
    "sbp": (50, 250),
    "dbp": (30, 160),
    "temperature": (30, 43),
}


def db_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_tables():
    conn = db_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS news2_scores (
            id BIGSERIAL PRIMARY KEY,
            score_time TIMESTAMPTZ DEFAULT NOW(),
            patient_id TEXT,
            patient_name TEXT,
            bed_number TEXT,
            physician TEXT,
            heart_rate NUMERIC,
            spo2 NUMERIC,
            resp_rate NUMERIC,
            sbp NUMERIC,
            dbp NUMERIC,
            temperature NUMERIC,
            avpu TEXT,
            on_oxygen BOOLEAN,
            news2_score INT,
            risk_category TEXT
        );
        """
    )

    conn.commit()
    cur.close()
    conn.close()


def validate_event(event):
    vital_type = event.get("vital_type")
    value = event.get("vital_value")

    if vital_type == "avpu":
        if value not in ["A", "V", "P", "U"]:
            return False, "AVPU invalid value"
        return True, "OK"

    if vital_type not in BOUNDS:
        return False, "Unknown vital type"

    try:
        value = float(value)
    except Exception:
        return False, f"{vital_type} non-numeric value"

    low, high = BOUNDS[vital_type]

    if vital_type == "heart_rate" and value == 0:
        return False, "HR probe-off or invalid zero reading"

    if vital_type == "spo2" and value == 0:
        return False, "SpO2 probe-off or invalid zero reading"

    if value < low or value > high:
        return False, f"{vital_type} physiological bound failure"

    return True, "OK"


def consume_events(max_messages):
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"news2_processor_{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([RAW_TOPIC])

    valid_events = []
    dq_events = []

    print("Reading events from Kafka topic raw_vitals...")

    idle_count = 0

    while len(valid_events) + len(dq_events) < max_messages and idle_count < 20:
        msg = consumer.poll(1.0)

        if msg is None:
            idle_count += 1
            continue

        if msg.error():
            print("Kafka message error:", msg.error())
            continue

        idle_count = 0

        event = json.loads(msg.value().decode("utf-8"))

        ok, reason = validate_event(event)

        if ok:
            valid_events.append(event)
        else:
            dq_events.append(
                {
                    "event_time": event.get("event_time"),
                    "patient_id": event.get("patient_id"),
                    "vital_type": event.get("vital_type"),
                    "vital_value": str(event.get("vital_value")),
                    "quality_reason": reason,
                }
            )

    consumer.close()

    print(f"Total consumed: {len(valid_events) + len(dq_events)}")
    print(f"Valid events: {len(valid_events)}")
    print(f"DQ events: {len(dq_events)}")

    return valid_events, dq_events


def produce_events(topic, events):
    if not events:
        return

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    for event in events:
        producer.produce(
            topic,
            key=event["patient_id"],
            value=json.dumps(event).encode("utf-8"),
        )

    producer.flush()


def save_dq_to_db(dq_events):
    if not dq_events:
        return

    rows = [
        (
            e["patient_id"],
            e["vital_type"],
            str(e["vital_value"]),
            e["quality_reason"],
        )
        for e in dq_events
    ]

    conn = db_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO dq_events
        (patient_id, vital_type, vital_value, quality_reason)
        VALUES %s
        """,
        rows,
        page_size=5000,
    )

    conn.commit()
    cur.close()
    conn.close()


def score_resp_rate(v):
    if pd.isna(v):
        return 0
    v = float(v)
    if v <= 8:
        return 3
    if v <= 11:
        return 1
    if v <= 20:
        return 0
    if v <= 24:
        return 2
    return 3


def score_spo2(v):
    if pd.isna(v):
        return 0
    v = float(v)
    if v <= 91:
        return 3
    if v <= 93:
        return 2
    if v <= 95:
        return 1
    return 0


def score_sbp(v):
    if pd.isna(v):
        return 0
    v = float(v)
    if v <= 90:
        return 3
    if v <= 100:
        return 2
    if v <= 110:
        return 1
    if v <= 219:
        return 0
    return 3


def score_heart_rate(v):
    if pd.isna(v):
        return 0
    v = float(v)
    if v <= 40:
        return 3
    if v <= 50:
        return 1
    if v <= 90:
        return 0
    if v <= 110:
        return 1
    if v <= 130:
        return 2
    return 3


def score_temperature(v):
    if pd.isna(v):
        return 0
    v = float(v)
    if v <= 35:
        return 3
    if v <= 36:
        return 1
    if v <= 38:
        return 0
    if v <= 39:
        return 1
    return 2


def score_avpu(v):
    return 0 if v == "A" else 3


def risk_category(score):
    if score >= 9:
        return "Critical"
    if score >= 7:
        return "High"
    if score >= 5:
        return "Medium"
    return "Low"


def compute_news2(valid_events):
    if not valid_events:
        return pd.DataFrame()

    df = pd.DataFrame(valid_events)

    df["event_second"] = pd.to_numeric(df["event_second"], errors="coerce")
    df = df.sort_values(["patient_id", "vital_type", "event_second"])

    latest = df.groupby(["patient_id", "vital_type"], as_index=False).tail(1)

    wide = latest.pivot(
        index="patient_id",
        columns="vital_type",
        values="vital_value",
    ).reset_index()

    metadata = (
        df.sort_values("event_second")
        .groupby("patient_id", as_index=False)
        .tail(1)[
            [
                "patient_id",
                "patient_name",
                "bed_number",
                "physician",
                "fhir_patient_id",
                "fhir_encounter_id",
                "on_oxygen",
            ]
        ]
    )

    result = metadata.merge(wide, on="patient_id", how="left")

    for col in ["heart_rate", "spo2", "resp_rate", "sbp", "dbp", "temperature"]:
        result[col] = pd.to_numeric(result.get(col), errors="coerce")

    result["avpu"] = result.get("avpu", "A")
    result["avpu"] = result["avpu"].fillna("A")
    result["on_oxygen"] = result["on_oxygen"].fillna(False).astype(bool)

    scores = []

    for _, row in result.iterrows():
        score = (
            score_resp_rate(row["resp_rate"])
            + score_spo2(row["spo2"])
            + score_sbp(row["sbp"])
            + score_heart_rate(row["heart_rate"])
            + score_temperature(row["temperature"])
            + score_avpu(row["avpu"])
            + (2 if bool(row["on_oxygen"]) else 0)
        )
        scores.append(int(score))

    result["news2_score"] = scores
    result["risk_category"] = result["news2_score"].apply(risk_category)

    return result


def clean_num(value):
    if pd.isna(value):
        return None
    return float(value)


def save_scores_to_db(scored_df):
    if scored_df.empty:
        return

    rows = []

    for _, row in scored_df.iterrows():
        rows.append(
            (
                row["patient_id"],
                row["patient_name"],
                row["bed_number"],
                row["physician"],
                clean_num(row["heart_rate"]),
                clean_num(row["spo2"]),
                clean_num(row["resp_rate"]),
                clean_num(row["sbp"]),
                clean_num(row["dbp"]),
                clean_num(row["temperature"]),
                row["avpu"],
                bool(row["on_oxygen"]),
                int(row["news2_score"]),
                row["risk_category"],
            )
        )

    conn = db_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO news2_scores
        (
            patient_id, patient_name, bed_number, physician,
            heart_rate, spo2, resp_rate, sbp, dbp, temperature,
            avpu, on_oxygen, news2_score, risk_category
        )
        VALUES %s
        """,
        rows,
        page_size=1000,
    )

    conn.commit()
    cur.close()
    conn.close()


def get_last_alert(patient_id):
    conn = db_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT alert_time, news2_score
        FROM alert_audit
        WHERE patient_id = %s
        ORDER BY alert_time DESC
        LIMIT 1
        """,
        (patient_id,),
    )

    row = cur.fetchone()

    cur.close()
    conn.close()

    return row


def should_alert(patient_id, score):
    last = get_last_alert(patient_id)

    if last is None:
        return True, "first NEWS2 threshold breach"

    last_time, last_score = last

    if score >= int(last_score) + 2:
        return True, "NEWS2 worsened by 2 or more points"

    if datetime.now(timezone.utc) - last_time >= timedelta(minutes=15):
        return True, "15-minute suppression window completed"

    return False, "suppressed to avoid alert fatigue"


def generate_alerts(scored_df):
    if scored_df.empty or "news2_score" not in scored_df.columns:
        return []

    alerts = []

    risky = scored_df[scored_df["news2_score"] >= 5].sort_values(
        "news2_score",
        ascending=False,
    )

    for _, row in risky.iterrows():
        fire, reason = should_alert(row["patient_id"], int(row["news2_score"]))

        if not fire:
            continue

        message = (
            f"Patient {row['patient_name']} in bed {row['bed_number']} has NEWS2 "
            f"{int(row['news2_score'])} ({row['risk_category']}). "
            f"Physician: {row['physician']}. "
            f"FHIR: {row['fhir_patient_id']} / {row['fhir_encounter_id']}."
        )

        alerts.append(
            {
                "alert_time": datetime.now(timezone.utc).isoformat(),
                "patient_id": row["patient_id"],
                "patient_name": row["patient_name"],
                "bed_number": row["bed_number"],
                "physician": row["physician"],
                "news2_score": int(row["news2_score"]),
                "risk_category": row["risk_category"],
                "alert_reason": reason,
                "alert_message": message,
            }
        )

    return alerts


def save_alerts_to_db(alerts):
    if not alerts:
        return

    rows = [
        (
            a["patient_id"],
            a["patient_name"],
            a["bed_number"],
            a["physician"],
            a["news2_score"],
            a["risk_category"],
            a["alert_reason"],
            a["alert_message"],
        )
        for a in alerts
    ]

    conn = db_conn()
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO alert_audit
        (
            patient_id, patient_name, bed_number, physician,
            news2_score, risk_category, alert_reason, alert_message
        )
        VALUES %s
        """,
        rows,
        page_size=1000,
    )

    conn.commit()
    cur.close()
    conn.close()


def run(max_messages):
    ensure_tables()

    valid_events, dq_events = consume_events(max_messages)

    produce_events(DQ_TOPIC, dq_events)
    save_dq_to_db(dq_events)

    scored_df = compute_news2(valid_events)
    save_scores_to_db(scored_df)

    alerts = generate_alerts(scored_df)
    produce_events(ALERT_TOPIC, alerts)
    save_alerts_to_db(alerts)

    print("\nNEWS2 stream processor completed.")
    print(f"Valid events processed: {len(valid_events)}")
    print(f"DQ events routed: {len(dq_events)}")
    print(f"Patients scored: {len(scored_df)}")
    print(f"Alerts generated: {len(alerts)}")

    if not scored_df.empty:
        print("\nRisk distribution:")
        print(scored_df["risk_category"].value_counts())

        print("\nTop 10 highest risk patients:")
        print(
            scored_df[
                [
                    "patient_id",
                    "patient_name",
                    "bed_number",
                    "news2_score",
                    "risk_category",
                    "heart_rate",
                    "spo2",
                    "resp_rate",
                    "sbp",
                    "temperature",
                    "avpu",
                ]
            ]
            .sort_values("news2_score", ascending=False)
            .head(10)
            .to_string(index=False)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-messages", type=int, default=30000)
    args = parser.parse_args()

    run(args.max_messages)
