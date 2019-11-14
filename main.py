import pandas as pd
import numpy as np
import datetime
import time
import math
from opyenxes.factory.XFactory import XFactory
from opyenxes.id.XIDFactory import XIDFactory
from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer


def timestamp_from_birthdate_and_age(date, age_in_days):
    if math.isnan(age_in_days):
        return date
    else:
        return date + datetime.timedelta(days=age_in_days)


def get_encounters_per_patient(patients, encounters):
    patient_mrns = patients.medical_record_number.unique()
    encounters_per_patient = {}
    for mrn in patient_mrns:
        encounters_for_patient = {}
        all_encounters_for_patient = encounters[(
            encounters.medical_record_number == mrn)]
        for index, encounter in all_encounters_for_patient.iterrows():
            if encounter['timestamp'] in encounters_for_patient:
                if encounter['timestamp_end'] > encounters_for_patient[encounter['timestamp']]['end_timestamp']:
                    encounters_for_patient[encounter['timestamp']] = {
                        "begin_timestamp": encounter['timestamp'],
                        "end_timestamp": encounter['timestamp_end'],
                        "mrn": encounter['medical_record_number']}
            else:
                encounters_for_patient[encounter['timestamp']] = {
                    "begin_timestamp": encounter['timestamp'],
                    "end_timestamp": encounter['timestamp_end'],
                    "mrn": encounter['medical_record_number']
                }
        encounters_per_patient[mrn] = encounters_for_patient
    return encounters_per_patient


def get_patient_events(patients, events):
    # join patients and events
    patient_events = pd.merge(
        patients, events, on='medical_record_number', how='outer')

    patient_events['timestamp'] = patient_events.apply(lambda row: timestamp_from_birthdate_and_age(
        row.date_of_birth, row.age_in_days), axis=1)

    pd.to_datetime(patient_events.timestamp, errors='coerce')
    return patient_events


def get_patient_encounters(patients, encounters):
    # join encounters and encounters
    patient_encounters = pd.merge(
        patients, encounters, on='medical_record_number', how='outer')

    patient_encounters['timestamp'] = patient_encounters.apply(lambda row: timestamp_from_birthdate_and_age(
        row.date_of_birth, row.age_in_days), axis=1)

    patient_encounters['timestamp_end'] = patient_encounters.apply(lambda row: timestamp_from_birthdate_and_age(
        row.timestamp, row.end_date_age_in_days), axis=1)

    pd.to_datetime(patient_encounters.timestamp, errors='coerce')
    return patient_encounters


def get_encounter_event_per_patient(patients, patient_encounter_buckets, events):
    patient_mrns = patients.medical_record_number.unique()
    encounter_events_per_patient = {}
    for mrn in patient_mrns:
        patient_encounters = patient_encounter_buckets[mrn]
        patient_events = events[(
            events.medical_record_number == mrn)]
        patient_events_per_encounter = {}
        for encounter_begin_date in patient_encounters:
            encounter_end_date = patient_encounters[encounter_begin_date]['end_timestamp']
            for index, event in patient_events.iterrows():
                if (event.timestamp == encounter_begin_date and event.timestamp < encounter_end_date) or (event.timestamp > encounter_begin_date and event.timestamp < encounter_end_date) or (event.timestamp > encounter_begin_date and event.timestamp == encounter_end_date) or (event.timestamp == encounter_begin_date and event.timestamp == encounter_end_date):
                    existing_events_per_encounter = patient_events_per_encounter.get(
                        encounter_begin_date)
                    if existing_events_per_encounter is None:
                        existing_events_per_encounter = [event]
                    else:
                        existing_events_per_encounter = existing_events_per_encounter + \
                            [event]
                    patient_events_per_encounter[encounter_begin_date] = existing_events_per_encounter
        encounter_events_per_patient[mrn] = patient_events_per_encounter
    return encounter_events_per_patient


def filter_encounter_events(encounter_events, condition):
    # iterate over MRN
    # iterate over encounter
    # iterate over events
    # if no event matches description, drop encounter
    return encounter_events


def create_log_from_filtered_encounter_events(filtered_encounter_events):
    # iterate over MRN
    # iterate over encounter
    # create trace per encounter
    # translate events to proper types
    # add events of encounter to trace

    # create log
    # https://github.com/opyenxes/OpyenXes/blob/master/example/Create_random_log.py
    # https://github.com/opyenxes/OpyenXes

    # https://github.com/maxsumrall/xes
    # log = XFactory.create_log()
    return ""


def translate_procedure_diagnosis_to_event():
    """
    When is diagnosis the event? When is procedure the event?

    encounter_type set
    context_diagnosis_code = MSDW_NOT_APPLICABLE | context_diagnosis_code = MSDW_UNKNOWN
    context_procedure_code set
    ->  procedure is event

    encounter_type set
    context_procedure_code = MSDW_NOT_APPLICABLE
    context_diagnosis_code set
    -> diagnosis is event
    """
    return ""


def log_from_cohort(cohort):
    # get necessary data from cohort
    patients = cohort.get(Patient())
    encounters = cohort.get(Encounter())
    events = cohort.get(Diagnosis(), Procedure())

    patient_events = get_patient_events(patients, events)

    patient_encounters = get_patient_encounters(patients, encounters)

    patient_encounter_buckets = get_encounters_per_patient(
        patients, patient_encounters)

    encounter_events_per_patient = get_encounter_event_per_patient(
        patients, patient_encounter_buckets, patient_events)

    filtered_encounter_events = filter_encounter_events(
        encounter_events_per_patient, "")

    log = create_log_from_filtered_encounter_events(filtered_encounter_events)

    """
        Encounter -> Case
        1. Get encounters for a patient
        2. Get procedures, diagnoses that happened for patient in timespan of encounter
        3. If one procedure/diagnosis of encounter matches condition
                -> Encounter (+ procedures/diagnoses) is part of case
    """

    return log  # Todo: make this XES file
