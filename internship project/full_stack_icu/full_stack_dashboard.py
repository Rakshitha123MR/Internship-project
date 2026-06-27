import requests
import numpy as np
import pandas as pd
import psycopg2
import plotly.express as px
import streamlit as st


DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "icu_db",
    "user": "icu_user",
    "password": "icu_pass",
}

FHIR_BASE = "http://localhost:8080/fhir"


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@st.cache_data(ttl=5)
def read_sql(query):
    conn = get_connection()
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()
    return df


@st.cache_data(ttl=5)
def get_count(table_name):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")
    value = cur.fetchone()[0]
    cur.close()
    conn.close()
    return value


@st.cache_data(ttl=5)
def get_latest_news2():
    query = """
        SELECT DISTINCT ON (patient_id)
            id, score_time, patient_id, patient_name, bed_number, physician,
            heart_rate, spo2, resp_rate, sbp, dbp, temperature,
            avpu, on_oxygen, news2_score, risk_category
        FROM news2_scores
        ORDER BY patient_id, score_time DESC;
    """
    return read_sql(query)


@st.cache_data(ttl=5)
def get_alerts():
    query = """
        SELECT alert_time, patient_id, patient_name, bed_number, physician,
               news2_score, risk_category, alert_reason, alert_message
        FROM alert_audit
        ORDER BY alert_time DESC
        LIMIT 200;
    """
    return read_sql(query)


@st.cache_data(ttl=5)
def get_dq_events():
    query = """
        SELECT event_time, patient_id, vital_type, vital_value, quality_reason
        FROM dq_events
        ORDER BY event_time DESC
        LIMIT 200;
    """
    return read_sql(query)


@st.cache_data(ttl=5)
def get_patient_trend(patient_id):
    query = f"""
        SELECT score_time, patient_id, heart_rate, spo2, resp_rate, sbp,
               dbp, temperature, news2_score, risk_category
        FROM news2_scores
        WHERE patient_id = '{patient_id}'
        ORDER BY score_time ASC;
    """
    return read_sql(query)


def fetch_fhir_context(patient_id):
    try:
        patient_response = requests.get(f"{FHIR_BASE}/Patient/{patient_id}", timeout=5)
        encounter_id = patient_id.replace("PAT", "ICU")
        encounter_response = requests.get(f"{FHIR_BASE}/Encounter/{encounter_id}", timeout=5)

        patient_data = patient_response.json()
        encounter_data = encounter_response.json()

        patient_name = "Unknown"
        if patient_data.get("resourceType") == "Patient":
            name = patient_data.get("name", [{}])[0]
            given = " ".join(name.get("given", []))
            family = name.get("family", "")
            patient_name = f"{given} {family}".strip()

        bed_number = encounter_id
        physician = "Unknown"

        if encounter_data.get("resourceType") == "Encounter":
            locations = encounter_data.get("location", [])
            if locations:
                bed_number = locations[0].get("location", {}).get("display", encounter_id)

            participants = encounter_data.get("participant", [])
            if participants:
                physician = participants[0].get("individual", {}).get("display", "Unknown")

        return {
            "Patient ID": patient_id,
            "Patient Name": patient_name,
            "Bed Number": bed_number,
            "Treating Physician": physician,
            "FHIR Patient Resource": f"Patient/{patient_id}",
            "FHIR Encounter Resource": f"Encounter/{encounter_id}",
        }

    except Exception as e:
        return {
            "Patient ID": patient_id,
            "FHIR Error": str(e),
        }


st.set_page_config(
    page_title="ICU NEWS2 Full-Stack Dashboard",
    layout="wide",
)

st.title("ICU Patient Deterioration Early Warning System")
st.caption("Full-stack charge nurse dashboard connected to Kafka, TimescaleDB/PostgreSQL, Parquet storage, and HAPI FHIR resources.")

if st.button("Refresh Dashboard"):
    st.cache_data.clear()
    st.rerun()

latest_df = get_latest_news2()
alerts_df = get_alerts()
dq_df = get_dq_events()

vitals_count = get_count("vitals_history")
dq_count = get_count("dq_events")
alert_count = get_count("alert_audit")
score_count = get_count("news2_scores")

if latest_df.empty:
    st.warning("No NEWS2 score data found yet. Run the producer and stream processor first.")
    st.stop()

high_critical_count = latest_df[latest_df["risk_category"].isin(["High", "Critical"])].shape[0]
medium_count = latest_df[latest_df["risk_category"] == "Medium"].shape[0]
low_count = latest_df[latest_df["risk_category"] == "Low"].shape[0]

m1, m2, m3, m4, m5, m6 = st.columns(6)

m1.metric("Patients Scored", latest_df["patient_id"].nunique())
m2.metric("Raw Vitals Stored", vitals_count)
m3.metric("NEWS2 Score Rows", score_count)
m4.metric("DQ Events", dq_count)
m5.metric("Total Alerts", alert_count)
m6.metric("High/Critical", high_critical_count)

st.subheader("Risk Category Summary")

risk_summary = latest_df["risk_category"].value_counts().reset_index()
risk_summary.columns = ["Risk Category", "Count"]

fig_risk = px.bar(
    risk_summary,
    x="Risk Category",
    y="Count",
    title="Patient Risk Category Distribution",
)

st.plotly_chart(fig_risk, use_container_width=True)

left, right = st.columns([1.15, 1])

with left:
    st.subheader("200-Patient NEWS2 Risk Heat Map")

    sorted_df = latest_df.sort_values("patient_id")
    scores = sorted_df["news2_score"].tolist()

    while len(scores) < 200:
        scores.append(0)

    heat_values = np.array(scores[:200]).reshape(10, 20)

    fig_heat = px.imshow(
        heat_values,
        aspect="auto",
        labels={
            "x": "ICU Bed Column",
            "y": "ICU Bed Row",
            "color": "NEWS2",
        },
        title="NEWS2 Score Per ICU Bed",
    )

    st.plotly_chart(fig_heat, use_container_width=True)

with right:
    st.subheader("Prioritised Patient Risk Queue")

    queue_df = latest_df.sort_values(
        ["news2_score", "spo2", "sbp"],
        ascending=[False, True, True],
    )

    st.dataframe(
        queue_df[
            [
                "patient_id",
                "patient_name",
                "bed_number",
                "physician",
                "news2_score",
                "risk_category",
                "heart_rate",
                "spo2",
                "resp_rate",
                "sbp",
                "temperature",
                "avpu",
            ]
        ].head(25),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Patient Vital Trend")

patient_options = sorted(latest_df["patient_id"].unique().tolist())
selected_patient = st.selectbox("Select Patient", patient_options)

trend_df = get_patient_trend(selected_patient)

if trend_df.empty:
    st.info("No trend data available for selected patient.")
else:
    selected_vital = st.selectbox(
        "Select Vital / Score",
        ["news2_score", "heart_rate", "spo2", "resp_rate", "sbp", "temperature"],
    )

    fig_trend = px.line(
        trend_df,
        x="score_time",
        y=selected_vital,
        markers=True,
        title=f"{selected_vital} trend for {selected_patient}",
    )

    st.plotly_chart(fig_trend, use_container_width=True)

st.subheader("FHIR Patient Context")

fhir_context = fetch_fhir_context(selected_patient)
st.json(fhir_context)

st.subheader("Alert History")

if alerts_df.empty:
    st.info("No alerts generated yet.")
else:
    st.dataframe(
        alerts_df,
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Data Quality Events")

if dq_df.empty:
    st.success("No data quality issues found.")
else:
    st.dataframe(
        dq_df,
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Project Stack Status")

stack_status = pd.DataFrame(
    [
        ["Python Bedside Monitor Simulator", "Completed"],
        ["Kafka raw_vitals Topic", "Completed"],
        ["Kafka dq_events Topic", "Completed"],
        ["Kafka alerts Topic", "Completed"],
        ["TimescaleDB Vitals History", "Completed"],
        ["PostgreSQL Alert Audit", "Completed"],
        ["Parquet Raw Storage", "Completed"],
        ["HAPI FHIR Patient Resources", "Completed"],
        ["HAPI FHIR Encounter Resources", "Completed"],
        ["Streamlit Charge Nurse Dashboard", "Completed"],
        ["Airflow Hourly Reports", "Pending"],
        ["PyFlink DataStream Layer", "Pending"],
    ],
    columns=["Requirement", "Status"],
)

st.dataframe(stack_status, use_container_width=True, hide_index=True)
