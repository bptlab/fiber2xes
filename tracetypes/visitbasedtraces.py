from collections import OrderedDict
import pandas as pd


def get_traces_per_patient_by_visit(patient_events, column_indices):
    return patient_event_encounters\
        .rdd\
        .map(lambda row: row + (row[column_indices["encounter_visit_id"]], ))\
        .toDF(list(patient_events.schema.names) + ["trace_id"])

    """
    class VisitBasedTraces(object):
    def get_traces_per_patient(patients, encounters, events):
        patient_encounters = VisitBasedTraces.get_patient_encounters(patients, encounters)
        # mrn -> encounter_visit_ids -> encounter_key
        patient_visits_and_encounters = VisitBasedTraces.get_visits_and_encounters_per_patient(
            patients, patient_encounters)
        # mrn -> encounter_visit_ids -> events
        return VisitBasedTraces.get_patient_events_per_visit(patients, patient_visits_and_encounters, events)

    def get_patient_encounters(patients, encounters):
        patient_encounters = pd.merge(
            patients, encounters, on='medical_record_number', how='inner')
        return patient_encounters

    def get_visits_and_encounters_per_patient(patients, encounters):
        patient_mrns = patients.medical_record_number.unique()
        visits_and_encounters_per_patient = {}
        for mrn in patient_mrns:
            visits_for_patient = {}
            all_visits_for_patient = encounters[(
                encounters.medical_record_number == mrn)].encounter_visit_id.unique()
            for encounter_visit_id in all_visits_for_patient:
                visits_for_patient[encounter_visit_id] = []
                all_encounter_keys = encounters[(
                    encounters.encounter_visit_id == encounter_visit_id)].encounter_key.unique()
                for encounter_key in all_encounter_keys:
                    visits_for_patient[encounter_visit_id] = visits_for_patient[encounter_visit_id] + [
                        encounter_key]
            visits_and_encounters_per_patient[mrn] = visits_for_patient
        return visits_and_encounters_per_patient

    def get_patient_events_per_visit(patients, patient_visits_and_encounters, patient_events):
        patient_mrns = patients.medical_record_number.unique()
        events_per_patient = {}
        for mrn in patient_mrns:
            events_per_patient[mrn] = {}
            patient_visits = patient_visits_and_encounters[mrn]
            for visit in patient_visits:
                events_per_patient[mrn][visit] = []
                encounters = patient_visits[visit]
                for encounter in encounters:
                    events = patient_events[(
                        patient_events.encounter_key == encounter)]
                    for index, event in events.iterrows():
                        events_per_patient[mrn][visit] = events_per_patient[mrn][visit] + [event]
        return events_per_patient
    """
