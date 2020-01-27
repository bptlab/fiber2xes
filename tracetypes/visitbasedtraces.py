import pandas as pd


class VisitBasedTraces(object):
    def get_traces_per_patient(patients, encounters, events):
        patient_mrns = patients.medical_record_number.unique()
        
        patient_encounters = VisitBasedTraces.get_patient_encounters(patients, encounters)
        # mrn -> encounter_visit_ids -> encounter_key
        encounter_key_to_visit_id = VisitBasedTraces.get_visits_and_encounters_per_patient(
            patient_mrns, patient_encounters)
        # mrn -> encounter_visit_ids -> events
        return VisitBasedTraces.get_patient_events_per_visit(patient_mrns, encounter_key_to_visit_id, events)

    def get_patient_encounters(patients, encounters):
        patient_encounters = pd.merge(
            patients, encounters, on='medical_record_number', how='inner')
        return patient_encounters

    def get_visits_and_encounters_per_patient(patient_mrns, encounters):
        encounter_key_to_visit_id = {}
        for index, encounter in encounters.iterrows():
            encounter_key_to_visit_id[encounter.encounter_key] = encounter.encounter_visit_id
        return encounter_key_to_visit_id

    def get_patient_events_per_visit(patient_mrns, encounter_key_to_visit_id, patient_events):
        events_per_patient = {}
        for mrn in patient_mrns:
            events_per_patient[mrn] = {}
        for index, event in patient_events.iterrows():
            if event.encounter_key in encounter_key_to_visit_id:
                visit_id = encounter_key_to_visit_id[event.encounter_key]
                if visit_id not in events_per_patient[event.medical_record_number]:
                    events_per_patient[event.medical_record_number][visit_id] = []
                events_per_patient[event.medical_record_number][visit_id].append(event)
        return events_per_patient
