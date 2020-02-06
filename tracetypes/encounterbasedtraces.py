import pandas as pd

# Deprecated
class EncounterBasedTraces(object):
    def get_traces_per_patient(patients, encounters, events):
        # mrn -> encounter_keys
        patient_encounters = EncounterBasedTraces.get_patient_encounters(patients, encounters)
        patient_encounter_keys = EncounterBasedTraces.get_encounter_keys_per_patient(
            patient_encounters)
        # mrn -> encounter_key -> events
        return EncounterBasedTraces.get_patient_events_per_encounter(patients, patient_encounter_keys, events)

    def get_patient_encounters(patients, encounters):
        patient_encounters = pd.merge(
            patients, encounters, on='medical_record_number', how='inner')
        return patient_encounters

    def get_encounter_keys_per_patient(patient_encounters):
        patient_mrns = patient_encounters.medical_record_number.unique()
        encounters_per_patient = {}
        for mrn in patient_mrns:
            encounters_for_patient = patient_encounters[(
                patient_encounters.medical_record_number == mrn)].encounter_key.unique()
            encounters_per_patient[mrn] = encounters_for_patient
        return encounters_per_patient

    def get_patient_events_per_encounter(patients, patient_encounters, patient_events):
        patient_mrns = patients.medical_record_number.unique()
        events_per_patient = {}
        for mrn in patient_mrns:
            events_per_patient[mrn] = {}
            encounters = patient_encounters[mrn]
            for encounter in encounters:
                events_per_patient[mrn][encounter] = []
                events = patient_events[(
                    patient_events.encounter_key == encounter)]
                for index, event in events.iterrows():
                    events_per_patient[mrn][encounter] = events_per_patient[mrn][encounter] + [event]
        return events_per_patient
