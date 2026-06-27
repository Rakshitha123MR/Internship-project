import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaProducer


KAFKA_BOOTSTRAP = "localhost:29092"
RAW_TOPIC = "raw_vitals"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "icu_db",
    "user": "icu_user",
    "password": "icu_pass",
}

FIRST_NAMES = ["Aarav", "Diya", "Vihaan", "Ananya", "Aditya", "Kavya", "Rohan", "Isha", "Arjun", "Meera"]
LAST_NAMES = ["Sharma", "Patil", "Rao", "Nair", "Gowda", "Khan", "Reddy", "Shetty", "Singh", "Joshi"]
DOCTORS = ["Dr. Rao", "Dr. Mehta", "Dr. Iyer", "Dr. Nair", "Dr. Kulkarni", "Dr. Sharma"]

VITAL_TYPES = ["heart_rate", "spo2", "resp_rate", "sbp", "dbp", "temperature", "avpu"]
DETERIORATING_PATIENTS = {5, 42, 117}
PROBE_OFF_PATIENTS = {73, 144}


def build_patients(total_patients=200):
    patients = []
    for i in range(1, total_patients + 1):
        patients.append(
            {
                "patient_id": f"PAT-{i:03d}",
                "patient_name": f"{FIRST_NAMES[i % len(FIRST_NAMES)]} {LAST_NAMES[i % len(LAST_NAMES)]}",
                "bed_number": f"ICU-{i:03d}",
                "physician": DOCTORS[i % len(DOCTORS)],
                "fhir_patient_id": f"Patient/PAT-{i:03d}",
                "fhir_encounter_id": f"Encounter/ICU-{i:03d}",
            }
        )
    return patients


def generate_values(rng, patient_number, second):
    minute = second / 60

    heart_rate = rng.normal(82, 10)
    spo2 = rng.normal(97, 1.3)
    resp_rate = rng.normal(17, 2.5)
    sbp = rng.normal(122, 12)
    dbp = rng.normal(78, 8)
    temperature = rng.normal(36.8, 0.3)
    avpu = "A"
    on_oxygen = rng.random() < 0.05

    if patient_number in DETERIORATING_PATIENTS:
        severity = min(1.0, max(0.0, (minute - 10) / 50))
        heart_rate += severity * 70
        spo2 -= severity * 15
        resp_rate += severity * 17
        sbp -= severity * 48
        dbp -= severity * 20
        temperature += severity * 2.1
        on_oxygen = severity > 0.35

        if severity > 0.75:
            avpu = "P"
        elif severity > 0.50:
            avpu = "V"

    cycle_second = second % 600
    if patient_number in PROBE_OFF_PATIENTS and 30 <= cycle_second <= 45:
        heart_rate = 0

    if rng.random() < 0.004:
        spo2 = 0
    if rng.random() < 0.004:
        resp_rate = float(rng.choice([2, 75, 90]))
    if rng.random() < 0.003:
        temperature = float(rng.choice([25, 46]))

    return {
        "heart_rate": round(float(heart_rate), 1),
        "spo2": round(float(spo2), 1),
        "resp_rate": round(float(resp_rate), 1),
        "sbp": round(float(sbp), 1),
        "dbp": round(float(dbp), 1),
        "temperature": round(float(temperature), 1),
        "avpu": avpu,
        "on_oxygen": bool(on_oxygen),
    }


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
    )


def insert_into_timescaledb(events):
    rows = [
        (
            event["event_time"],
            event["patient_id"],
            event["vital_type"],
            str(event["vital_value"]),
            event["arrival_time"],
            "RAW",
        )
        for event in events
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO vitals_history
        (event_time, patient_id, vital_type, vital_value, arrival_time, quality_status)
        VALUES %s
        """,
        rows,
        page_size=5000,
    )

    conn.commit()
    cur.close()
    conn.close()


def write_parquet(events):
    output_dir = Path("data/raw_parquet")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"raw_vitals_{timestamp}.parquet"

    df = pd.DataFrame(events)
    df["vital_value"] = df["vital_value"].astype(str)
    df["on_oxygen"] = df["on_oxygen"].astype(bool)
    df.to_parquet(output_file, index=False)

    return output_file


def run_producer(seconds=20, sleep_between_seconds=0.01, start_second=0):
    patients = build_patients()
    producer = create_kafka_producer()

    all_events = []
    event_count = 0

    print("Starting ICU bedside monitor producer...")
    print(f"Patients: {len(patients)}")
    print(f"Signals per patient: {len(VITAL_TYPES)}")
    print(f"Target topic: {RAW_TOPIC}")
    print(f"Duration: {seconds} seconds")
    print(f"Start second: {start_second}")

    for second in range(start_second, start_second + seconds):
        rng = np.random.default_rng(2026 + second)

        for patient in patients:
            patient_number = int(patient["patient_id"].split("-")[1])
            values = generate_values(rng, patient_number, second)

            event_time = datetime.now(timezone.utc).isoformat()
            arrival_time = datetime.now(timezone.utc).isoformat()

            for vital_type in VITAL_TYPES:
                event = {
                    "event_time": event_time,
                    "arrival_time": arrival_time,
                    "event_second": second,
                    "patient_id": patient["patient_id"],
                    "patient_name": patient["patient_name"],
                    "bed_number": patient["bed_number"],
                    "physician": patient["physician"],
                    "fhir_patient_id": patient["fhir_patient_id"],
                    "fhir_encounter_id": patient["fhir_encounter_id"],
                    "vital_type": vital_type,
                    "vital_value": values[vital_type],
                    "on_oxygen": values["on_oxygen"],
                    "source_monitor_id": f"MON-{patient['patient_id']}",
                }

                producer.send(
                    RAW_TOPIC,
                    key=patient["patient_id"],
                    value=event,
                )

                all_events.append(event)
                event_count += 1

        producer.flush()
        completed = second - start_second + 1
        print(f"Second {completed}/{seconds} completed. Total events: {event_count}")
        time.sleep(sleep_between_seconds)

    producer.flush()
    producer.close()

    parquet_file = write_parquet(all_events)
    insert_into_timescaledb(all_events)

    print("\nProducer completed successfully.")
    print(f"Total raw events generated: {len(all_events)}")
    print(f"Kafka topic used: {RAW_TOPIC}")
    print(f"Parquet file written: {parquet_file}")
    print("TimescaleDB table updated: vitals_history")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--start-second", type=int, default=0)
    args = parser.parse_args()

    run_producer(seconds=args.seconds, start_second=args.start_second)
