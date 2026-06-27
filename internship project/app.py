import sqlite3
from datetime import datetime
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = "icu_alerts.db"

FIRST_NAMES = ["Aarav", "Diya", "Vihaan", "Ananya", "Aditya", "Kavya", "Rohan", "Isha", "Arjun", "Meera"]
LAST_NAMES = ["Sharma", "Patil", "Rao", "Nair", "Gowda", "Khan", "Reddy", "Shetty", "Singh", "Joshi"]
DOCTORS = ["Dr. Rao", "Dr. Mehta", "Dr. Iyer", "Dr. Nair", "Dr. Kulkarni", "Dr. Sharma"]
VITAL_COLUMNS = ["heart_rate", "spo2", "resp_rate", "sbp", "dbp", "temperature"]
DETERIORATING_PATIENTS = {5, 42, 117}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            system_time TEXT,
            sim_minute INTEGER,
            patient_id TEXT,
            patient_name TEXT,
            bed TEXT,
            physician TEXT,
            news2_score INTEGER,
            risk_category TEXT,
            message TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def save_alerts(alerts):
    if not alerts:
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    for alert in alerts:
        cur.execute(
            """
            INSERT INTO alert_audit
            (system_time, sim_minute, patient_id, patient_name, bed, physician, news2_score, risk_category, message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert["system_time"],
                alert["sim_minute"],
                alert["patient_id"],
                alert["patient_name"],
                alert["bed"],
                alert["physician"],
                int(alert["news2_score"]),
                alert["risk_category"],
                alert["message"],
            ),
        )
    conn.commit()
    conn.close()


def read_alerts(limit=200):
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM alert_audit ORDER BY id DESC LIMIT ?",
            conn,
            params=(limit,),
        )
    finally:
        conn.close()
    return df


def clear_alerts():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM alert_audit")
    conn.commit()
    conn.close()


def generate_patient_master(n=200):
    rows = []
    for i in range(1, n + 1):
        name = f"{FIRST_NAMES[i % len(FIRST_NAMES)]} {LAST_NAMES[i % len(LAST_NAMES)]}"
        rows.append(
            {
                "patient_id": f"PAT-{i:03d}",
                "patient_name": name,
                "bed": f"ICU-{i:03d}",
                "physician": DOCTORS[i % len(DOCTORS)],
            }
        )
    return pd.DataFrame(rows)


def simulate_snapshot(patients, sim_minute):
    rng = np.random.default_rng(2026 + int(sim_minute))
    rows = []
    for _, patient in patients.iterrows():
        patient_num = int(patient["patient_id"].split("-")[1])
        heart_rate = rng.normal(82, 12)
        spo2 = rng.normal(97, 1.5)
        resp_rate = rng.normal(17, 3)
        sbp = rng.normal(122, 14)
        dbp = rng.normal(78, 8)
        temperature = rng.normal(36.8, 0.35)
        avpu = "A"
        on_oxygen = rng.random() < 0.05

        if patient_num in DETERIORATING_PATIENTS:
            severity = min(1.0, max(0.0, (sim_minute - 10) / 50))
            heart_rate += severity * 65
            spo2 -= severity * 14
            resp_rate += severity * 16
            sbp -= severity * 45
            dbp -= severity * 20
            temperature += severity * 2.0
            on_oxygen = severity > 0.35
            if severity > 0.75:
                avpu = "P"
            elif severity > 0.50:
                avpu = "V"

        dq_hint = []
        if rng.random() < 0.015:
            heart_rate = 0
            dq_hint.append("simulated HR probe off")
        if rng.random() < 0.015:
            spo2 = 0
            dq_hint.append("simulated SpO2 probe off")
        if rng.random() < 0.020:
            resp_rate = rng.choice([2, 75, 90])
            dq_hint.append("simulated respiratory artefact")
        if rng.random() < 0.015:
            temperature = rng.choice([25, 46])
            dq_hint.append("simulated temperature artefact")

        rows.append(
            {
                "sim_minute": int(sim_minute),
                "patient_id": patient["patient_id"],
                "patient_name": patient["patient_name"],
                "bed": patient["bed"],
                "physician": patient["physician"],
                "heart_rate": round(float(heart_rate), 1),
                "spo2": round(float(spo2), 1),
                "resp_rate": round(float(resp_rate), 1),
                "sbp": round(float(sbp), 1),
                "dbp": round(float(dbp), 1),
                "temperature": round(float(temperature), 1),
                "avpu": avpu,
                "on_oxygen": bool(on_oxygen),
                "simulated_issue": ", ".join(dq_hint) if dq_hint else "None",
            }
        )
    return pd.DataFrame(rows)


def clean_and_validate(df):
    df = df.copy()
    dq_status = []
    for idx, row in df.iterrows():
        flags = []
        if row["heart_rate"] == 0:
            flags.append("HR probe off")
        if row["spo2"] == 0:
            flags.append("SpO2 probe off")
        if not (20 <= row["heart_rate"] <= 250):
            flags.append("HR invalid")
            df.at[idx, "heart_rate"] = np.nan
        if not (50 <= row["spo2"] <= 100):
            flags.append("SpO2 invalid")
            df.at[idx, "spo2"] = np.nan
        if not (4 <= row["resp_rate"] <= 60):
            flags.append("RR invalid")
            df.at[idx, "resp_rate"] = np.nan
        if not (50 <= row["sbp"] <= 250):
            flags.append("SBP invalid")
            df.at[idx, "sbp"] = np.nan
        if not (30 <= row["dbp"] <= 160):
            flags.append("DBP invalid")
            df.at[idx, "dbp"] = np.nan
        if not (30 <= row["temperature"] <= 43):
            flags.append("Temperature invalid")
            df.at[idx, "temperature"] = np.nan
        if row["avpu"] not in ["A", "V", "P", "U"]:
            flags.append("AVPU invalid")
            df.at[idx, "avpu"] = "A"
        dq_status.append("OK" if not flags else ", ".join(sorted(set(flags))))
    df["dq_status"] = dq_status
    return df


def impute_recent_values(current_df, history_df):
    current_df = current_df.copy()
    if history_df.empty:
        return current_df
    sim_minute = int(current_df["sim_minute"].iloc[0])
    recent_history = history_df[history_df["sim_minute"] >= sim_minute - 5]
    if recent_history.empty:
        return current_df
    for idx, row in current_df.iterrows():
        patient_recent = recent_history[recent_history["patient_id"] == row["patient_id"]].sort_values("sim_minute")
        if patient_recent.empty:
            continue
        for col in VITAL_COLUMNS:
            if pd.isna(row[col]):
                valid = patient_recent[col].dropna()
                if not valid.empty:
                    current_df.at[idx, col] = valid.iloc[-1]
                    current_df.at[idx, "dq_status"] = current_df.at[idx, "dq_status"] + f", imputed {col}"
    return current_df


def score_resp_rate(value):
    if pd.isna(value):
        return 0
    if value <= 8:
        return 3
    if value <= 11:
        return 1
    if value <= 20:
        return 0
    if value <= 24:
        return 2
    return 3


def score_spo2(value):
    if pd.isna(value):
        return 0
    if value <= 91:
        return 3
    if value <= 93:
        return 2
    if value <= 95:
        return 1
    return 0


def score_sbp(value):
    if pd.isna(value):
        return 0
    if value <= 90:
        return 3
    if value <= 100:
        return 2
    if value <= 110:
        return 1
    if value <= 219:
        return 0
    return 3


def score_heart_rate(value):
    if pd.isna(value):
        return 0
    if value <= 40:
        return 3
    if value <= 50:
        return 1
    if value <= 90:
        return 0
    if value <= 110:
        return 1
    if value <= 130:
        return 2
    return 3


def score_temperature(value):
    if pd.isna(value):
        return 0
    if value <= 35.0:
        return 3
    if value <= 36.0:
        return 1
    if value <= 38.0:
        return 0
    if value <= 39.0:
        return 1
    return 2


def score_avpu(value):
    return 0 if value == "A" else 3


def risk_category(score):
    if score >= 9:
        return "Critical"
    if score >= 7:
        return "High"
    if score >= 5:
        return "Medium"
    return "Low"


def add_news2_scores(df):
    df = df.copy()
    scores = []
    for _, row in df.iterrows():
        total = (
            score_resp_rate(row["resp_rate"])
            + score_spo2(row["spo2"])
            + score_sbp(row["sbp"])
            + score_heart_rate(row["heart_rate"])
            + score_temperature(row["temperature"])
            + score_avpu(row["avpu"])
            + (2 if row["on_oxygen"] else 0)
        )
        scores.append(int(total))
    df["news2_score"] = scores
    df["risk_category"] = df["news2_score"].apply(risk_category)
    return df


def evaluate_alerts(scored_df, alert_state, sim_minute):
    alerts = []
    suppressed = 0
    risky = scored_df[scored_df["news2_score"] >= 5].sort_values("news2_score", ascending=False)
    for _, row in risky.iterrows():
        patient_id = row["patient_id"]
        score = int(row["news2_score"])
        previous = alert_state.get(patient_id)
        fire = False
        reason = ""
        if previous is None:
            fire = True
            reason = "first NEWS2 threshold breach"
        elif score >= previous["last_score"] + 2:
            fire = True
            reason = "NEWS2 worsened by 2 or more points"
        elif sim_minute - previous["last_alert_minute"] >= 15:
            fire = True
            reason = "suppression window completed"
        else:
            suppressed += 1
        if fire:
            message = (
                f"{row['patient_name']} in {row['bed']} has NEWS2 {score} "
                f"({row['risk_category']}). Alert reason: {reason}. Treating physician: {row['physician']}."
            )
            alerts.append(
                {
                    "system_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "sim_minute": int(sim_minute),
                    "patient_id": patient_id,
                    "patient_name": row["patient_name"],
                    "bed": row["bed"],
                    "physician": row["physician"],
                    "news2_score": score,
                    "risk_category": row["risk_category"],
                    "message": message,
                }
            )
            alert_state[patient_id] = {"last_alert_minute": int(sim_minute), "last_score": score}
    return alerts, suppressed


def process_simulation_step(sim_minute):
    raw = simulate_snapshot(st.session_state.patients, sim_minute)
    cleaned = clean_and_validate(raw)
    imputed = impute_recent_values(cleaned, st.session_state.history)
    scored = add_news2_scores(imputed)
    alerts, suppressed = evaluate_alerts(scored, st.session_state.alert_state, sim_minute)
    save_alerts(alerts)
    st.session_state.latest = scored
    st.session_state.last_alerts = alerts
    st.session_state.suppressed_alerts = suppressed
    st.session_state.history = pd.concat([st.session_state.history, scored], ignore_index=True)
    st.session_state.history = st.session_state.history[st.session_state.history["sim_minute"] >= sim_minute - 60]


st.set_page_config(page_title="ICU Early Warning System", layout="wide")
st.title("ICU Patient Deterioration Early Warning System")
st.caption("Academic prototype for real-time vital stream simulation, NEWS2 scoring, alert suppression, audit logging, and charge nurse dashboard.")

init_db()

if "patients" not in st.session_state:
    st.session_state.patients = generate_patient_master(200)
if "history" not in st.session_state:
    st.session_state.history = pd.DataFrame()
if "alert_state" not in st.session_state:
    st.session_state.alert_state = {}
if "sim_minute" not in st.session_state:
    st.session_state.sim_minute = 0
if "latest" not in st.session_state:
    process_simulation_step(st.session_state.sim_minute)

with st.sidebar:
    st.header("Simulation Control")
    st.write(f"Current simulation minute: {st.session_state.sim_minute}")
    if st.button("Generate next 2-minute window"):
        st.session_state.sim_minute += 2
        process_simulation_step(st.session_state.sim_minute)
    if st.button("Run 1-hour demo"):
        for _ in range(30):
            st.session_state.sim_minute += 2
            process_simulation_step(st.session_state.sim_minute)
    if st.button("Reset simulation and alert database"):
        clear_alerts()
        for key in ["patients", "history", "alert_state", "sim_minute", "latest", "last_alerts", "suppressed_alerts"]:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()
    selected_vital = st.selectbox("Trend vital sign", ["heart_rate", "spo2", "resp_rate", "sbp", "temperature"])

latest = st.session_state.latest.copy()
alert_history = read_alerts(200)
current_alerts = len(st.session_state.get("last_alerts", []))
suppressed_alerts = int(st.session_state.get("suppressed_alerts", 0))
high_risk_count = int((latest["risk_category"].isin(["High", "Critical"])).sum())
dq_count = int((latest["dq_status"] != "OK").sum())

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Total ICU Patients", len(latest))
m2.metric("High/Critical Patients", high_risk_count)
m3.metric("Alerts Fired Now", current_alerts)
m4.metric("Suppressed Alerts", suppressed_alerts)
m5.metric("DQ Issues Now", dq_count)

if current_alerts > 0:
    st.error(f"{current_alerts} clinical alert(s) generated in the current 2-minute window.")
else:
    st.success("No new clinical alerts in the current 2-minute window.")

left, right = st.columns([1.2, 1])

with left:
    st.subheader("200-Patient NEWS2 Risk Heat Map")
    heat_values = latest.sort_values("patient_id")["news2_score"].to_numpy().reshape(10, 20)
    fig_heat = px.imshow(
        heat_values,
        aspect="auto",
        labels={"x": "ICU bed column", "y": "ICU bed row", "color": "NEWS2"},
        title="Each cell represents one ICU patient bed",
    )
    st.plotly_chart(fig_heat, use_container_width=True)

with right:
    st.subheader("Ranked Patient Risk Queue")
    queue = latest.sort_values(["news2_score", "spo2", "sbp"], ascending=[False, True, True])
    st.dataframe(
        queue[["patient_id", "patient_name", "bed", "physician", "news2_score", "risk_category", "heart_rate", "spo2", "resp_rate", "sbp", "temperature", "avpu"]].head(20),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Top-10 Highest Risk Patient Vital Trend")
top_patient_ids = queue.head(10)["patient_id"].tolist()
trend_df = st.session_state.history[st.session_state.history["patient_id"].isin(top_patient_ids)]
if not trend_df.empty:
    fig_trend = px.line(
        trend_df,
        x="sim_minute",
        y=selected_vital,
        color="patient_id",
        markers=True,
        title=f"{selected_vital} trend for top-10 highest risk patients",
    )
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("Trend data will appear after simulation starts.")

st.subheader("Current Alert Messages")
if st.session_state.get("last_alerts"):
    st.dataframe(pd.DataFrame(st.session_state.last_alerts), use_container_width=True, hide_index=True)
else:
    st.info("No alert fired in the latest window.")

st.subheader("Immutable Alert Audit Log")
if alert_history.empty:
    st.info("No audit records yet.")
else:
    st.dataframe(alert_history, use_container_width=True, hide_index=True)

st.subheader("Data Quality Log")
dq_df = latest[latest["dq_status"] != "OK"]
if dq_df.empty:
    st.success("All current readings passed validation.")
else:
    st.dataframe(
        dq_df[["sim_minute", "patient_id", "patient_name", "bed", "dq_status", "simulated_issue", "heart_rate", "spo2", "resp_rate", "sbp", "temperature"]],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Project Modules Achieved")
st.write(
    "This prototype demonstrates patient stream simulation, sensor data-quality validation, recent-value imputation, "
    "NEWS2 scoring, risk categorisation, alert suppression, SQLite audit logging, and Streamlit-based charge nurse monitoring."
)
