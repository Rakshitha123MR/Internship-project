import time
import requests


FHIR_BASE_CANDIDATES = [
    "http://localhost:8080/fhir",
    "http://localhost:8080",
]

FIRST_NAMES = ["Aarav", "Diya", "Vihaan", "Ananya", "Aditya", "Kavya", "Rohan", "Isha", "Arjun", "Meera"]
LAST_NAMES = ["Sharma", "Patil", "Rao", "Nair", "Gowda", "Khan", "Reddy", "Shetty", "Singh", "Joshi"]

DOCTORS = [
    "Dr. Rao",
    "Dr. Mehta",
    "Dr. Iyer",
    "Dr. Nair",
    "Dr. Kulkarni",
    "Dr. Sharma",
]

DOCTOR_IDS = {
    "Dr. Rao": "DR-RAO",
    "Dr. Mehta": "DR-MEHTA",
    "Dr. Iyer": "DR-IYER",
    "Dr. Nair": "DR-NAIR",
    "Dr. Kulkarni": "DR-KULKARNI",
    "Dr. Sharma": "DR-SHARMA",
}

HEADERS = {
    "Content-Type": "application/fhir+json",
    "Accept": "application/fhir+json",
}


def find_fhir_base():
    for _ in range(30):
        for base in FHIR_BASE_CANDIDATES:
            try:
                r = requests.get(f"{base}/metadata", timeout=5)
                if r.status_code == 200:
                    return base
            except Exception:
                pass

        print("Waiting for HAPI FHIR server...")
        time.sleep(5)

    raise RuntimeError("HAPI FHIR server is not responding.")


def build_practitioner(doctor_name):
    doctor_id = DOCTOR_IDS[doctor_name]
    clean_name = doctor_name.replace("Dr. ", "")

    return {
        "resourceType": "Practitioner",
        "id": doctor_id,
        "identifier": [
            {
                "system": "http://hospital.example.org/icu/practitioner-id",
                "value": doctor_id,
            }
        ],
        "name": [
            {
                "use": "official",
                "family": clean_name,
                "given": ["Dr"],
                "text": doctor_name,
            }
        ],
        "active": True,
    }


def build_patient(i):
    patient_id = f"PAT-{i:03d}"
    first_name = FIRST_NAMES[i % len(FIRST_NAMES)]
    last_name = LAST_NAMES[i % len(LAST_NAMES)]

    return {
        "resourceType": "Patient",
        "id": patient_id,
        "identifier": [
            {
                "system": "http://hospital.example.org/icu/patient-id",
                "value": patient_id,
            }
        ],
        "name": [
            {
                "use": "official",
                "family": last_name,
                "given": [first_name],
            }
        ],
        "gender": "unknown",
        "active": True,
    }


def build_encounter(i):
    patient_id = f"PAT-{i:03d}"
    encounter_id = f"ICU-{i:03d}"
    doctor = DOCTORS[i % len(DOCTORS)]
    doctor_id = DOCTOR_IDS[doctor]

    return {
        "resourceType": "Encounter",
        "id": encounter_id,
        "status": "in-progress",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "IMP",
            "display": "inpatient encounter",
        },
        "subject": {
            "reference": f"Patient/{patient_id}",
            "display": patient_id,
        },
        "participant": [
            {
                "individual": {
                    "reference": f"Practitioner/{doctor_id}",
                    "display": doctor,
                }
            }
        ],
        "location": [
            {
                "location": {
                    "display": encounter_id,
                }
            }
        ],
    }


def put_resource(base, resource_type, resource_id, payload):
    url = f"{base}/{resource_type}/{resource_id}"
    response = requests.put(url, json=payload, headers=HEADERS, timeout=20)

    if response.status_code not in [200, 201]:
        print(f"Failed: {resource_type}/{resource_id} -> {response.status_code}")
        print(response.text[:500])
        return False

    return True


def main():
    base = find_fhir_base()
    print(f"HAPI FHIR base found: {base}")

    practitioner_count = 0
    patient_count = 0
    encounter_count = 0

    for doctor in DOCTORS:
        practitioner = build_practitioner(doctor)

        if put_resource(base, "Practitioner", practitioner["id"], practitioner):
            practitioner_count += 1

    for i in range(1, 201):
        patient = build_patient(i)

        if put_resource(base, "Patient", patient["id"], patient):
            patient_count += 1

    for i in range(1, 201):
        encounter = build_encounter(i)

        if put_resource(base, "Encounter", encounter["id"], encounter):
            encounter_count += 1

    print("\nFHIR seeding completed.")
    print(f"Practitioners inserted: {practitioner_count}")
    print(f"Patients inserted: {patient_count}")
    print(f"Encounters inserted: {encounter_count}")

    sample_patient = requests.get(f"{base}/Patient/PAT-001", headers=HEADERS).json()
    sample_encounter = requests.get(f"{base}/Encounter/ICU-001", headers=HEADERS).json()
    sample_practitioner = requests.get(f"{base}/Practitioner/DR-MEHTA", headers=HEADERS).json()

    print("\nSample Patient:")
    print(sample_patient.get("resourceType"), sample_patient.get("id"), sample_patient.get("name"))

    print("\nSample Practitioner:")
    print(sample_practitioner.get("resourceType"), sample_practitioner.get("id"), sample_practitioner.get("name"))

    print("\nSample Encounter:")
    print(sample_encounter.get("resourceType"), sample_encounter.get("id"), sample_encounter.get("participant"), sample_encounter.get("location"))


if __name__ == "__main__":
    main()
