import os
import re
import sys
import functools
import csv
import pandas as pd
import numpy as np
import datetime
from enum import Enum
import time
import math
import uuid

from opyenxes.factory.XFactory import XFactory
from opyenxes.id.XIDFactory import XIDFactory
from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer

from fiber import Cohort
from fiber.condition import (
    Procedure,
    Patient,
    Diagnosis,
    VitalSign,
    LabValue,
    Encounter,
    Measurement
)

from .translation import Translation
from .abstraction import Abstraction
from .fiberpatch import (
    DiagnosisWithTime,
    DrugWithTime,
    EncounterWithVisit,
    MaterialWithTime,
    PatientWithAttributes,
    ProcedureWithTime
)


def timer(func):
    # Decorator to benchmark functions
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} s")
        return value
    return wrapper_timer


class EventType(Enum):
    DIAGNOSIS = 0
    PROCEDURE = 1
    MATERIAL = 2


def timestamp_from_birthdate_and_age_and_time(date, age_in_days, time_of_day_key):
    if math.isnan(age_in_days) or date == "MSDW_UNKOWN" or age_in_days == "MSDW_UNKNOWN":
        return None
    else:
        timestamp_without_hours_and_minutes = date + \
            datetime.timedelta(days=age_in_days)
        date_without_time = pd.to_datetime(
            timestamp_without_hours_and_minutes, errors='coerce')
        date_with_time = date_without_time + \
            datetime.timedelta(minutes=time_of_day_key)
        return date_with_time


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


def get_encounter_keys_per_patient(patient_encounters):
    patient_mrns = patient_encounters.medical_record_number.unique()
    encounters_per_patient = {}
    for mrn in patient_mrns:
        encounters_for_patient = patient_encounters[(
            patient_encounters.medical_record_number == mrn)].encounter_key.unique()
        encounters_per_patient[mrn] = encounters_for_patient
    return encounters_per_patient


@timer
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


def get_patient_encounters(patients, encounters):
    patient_encounters = pd.merge(
        patients, encounters, on='medical_record_number', how='inner')
    return patient_encounters


@timer
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


@timer
def get_patient_events_per_patient(patients, patient_events):
    patient_mrns = patients.medical_record_number.unique()
    events_per_patient = {}
    for mrn in patient_mrns:
        events_per_patient[mrn] = {}
        events_per_patient[mrn][mrn] = []
        events = patient_events[(patient_events.medical_record_number == mrn)]
        for index, event in events.iterrows():
            events_per_patient[mrn][mrn] = events_per_patient[mrn][mrn] + [event]
    return events_per_patient


@timer
def get_patient_events(patients, events):
    # join patients and events
    patient_events = pd.merge(
        patients, events, on='medical_record_number', how='inner')

    patient_events['timestamp'] = patient_events.apply(lambda row: timestamp_from_birthdate_and_age_and_time(
        row.date_of_birth, row.age_in_days, row.time_of_day_key), axis=1)

    indexes_to_drop = []
    unique_events = set()
    for index, event in patient_events.iterrows():
        tup = (event["medical_record_number"], event["timestamp"],
               event["context_diagnosis_code"], event["context_procedure_code"])
        if tup not in unique_events and event["timestamp"] is not None:
            unique_events.add(tup)
        else:
            indexes_to_drop.append(index)
    patient_events.drop(patient_events.index[indexes_to_drop], inplace=True)
    return patient_events


@timer
def filter_events(events_to_filter, trace_filter=None):
    # iterate over MRN
    # iterate over trace keys
    # iterate over events
    # if no event matches description, drop encounter

    filtered_events = {}
    for mrn in events_to_filter:
        for trace_key in events_to_filter[mrn]:
            is_relevant = False
            if trace_filter is None:
                is_relevant = True
            else:
                is_relevant = trace_filter.is_relevant_trace(
                    events_to_filter[mrn][trace_key])

            if is_relevant:
                if mrn not in filtered_events:
                    filtered_events[mrn] = {}
                if trace_key not in filtered_events[mrn]:
                    filtered_events[mrn][trace_key] = {}
                filtered_events[mrn][trace_key] = events_to_filter[mrn][trace_key]

    return filtered_events


@timer
def create_log_from_filtered_events(filtered_events, verbose, remove_unlisted, event_filter, patients):
    # iterate over MRN
    # iterate over encounter
    # create trace per encounter
    # translate events to proper types
    # add events of encounter to trace

    log = XFactory.create_log()
    for mrn in filtered_events:
        trace_id = 0
        patient_data = patients.loc[patients["medical_record_number"] == mrn]
        for trace_key in filtered_events[mrn]:
            trace = XFactory.create_trace()

            id_attribute = XFactory.create_attribute_id(
                "id", str(uuid.uuid4()))
            trace.get_attributes()["id"] = id_attribute

            if patient_data is not None:
                trace.get_attributes()["patient:date_of_birth"] = XFactory.create_attribute_literal(
                    "patient:date_of_birth", patient_data["date_of_birth"].values[0])
                trace.get_attributes()["patient:address_zip"] = XFactory.create_attribute_literal(
                    "patient:address_zip", patient_data["address_zip"].values[0])
                trace.get_attributes()["patient:gender"] = XFactory.create_attribute_literal(
                    "patient:gender", patient_data["gender"].values[0])
                trace.get_attributes()["patient:language"] = XFactory.create_attribute_literal(
                    "patient:language", patient_data["language"].values[0])
                trace.get_attributes()["patient:patient_ethnic_group"] = XFactory.create_attribute_literal(
                    "patient:patient_ethnic_group", patient_data["patient_ethnic_group"].values[0])
                trace.get_attributes()["patient:race"] = XFactory.create_attribute_literal(
                    "patient:race", patient_data["race"].values[0])
                trace.get_attributes()["patient:religion"] = XFactory.create_attribute_literal(
                    "patient:religion", patient_data["religion"].values[0])
                trace.get_attributes()["patient:citizenship"] = XFactory.create_attribute_literal(
                    "patient:citizenship", patient_data["citizenship"].values[0])
                trace.get_attributes()["patient:marital_status_code"] = XFactory.create_attribute_literal(
                    "patient:marital_status_code", patient_data["marital_status_code"].values[0])
            trace_id = trace_id + 1

            for event in filtered_events[mrn][trace_key]:
                is_relevant = False
                if event_filter is None:
                    is_relevant = True
                else:
                    is_relevant = event_filter.is_relevant_event(event)

                if not is_relevant:
                    continue

                event_descriptor, event_name, event_context, event_code = translate_procedure_diagnosis_material_to_event(
                    event=event,
                    verbose=verbose,
                    remove_unlisted=remove_unlisted
                )
                if event_descriptor is not None:
                    log_event = XFactory.create_event()

                    timestamp_int = event.timestamp
                    timestamp_attribute = XFactory.create_attribute_timestamp(
                        "time:timestamp", timestamp_int)
                    log_event.get_attributes()["timestamp"] = timestamp_attribute
                    
                    activity_attribute = XFactory.create_attribute_literal(
                        "concept:name", event_descriptor)
                    log_event.get_attributes()["Activity"] = activity_attribute

                    description_attribute = XFactory.create_attribute_literal(
                        "event:description", event_name)
                    log_event.get_attributes()["event:description"] = description_attribute

                    context_attribute = XFactory.create_attribute_literal(
                        "event:context", event_context)
                    log_event.get_attributes()["event:context"] = context_attribute

                    code_attribute = XFactory.create_attribute_literal(
                        "event:code", event_code)
                    log_event.get_attributes()["event:code"] = code_attribute

                    trace.append(log_event)
            if len(trace) > 0:
                log.append(trace)
    return log


def translate_procedure_diagnosis_material_to_event(event, verbose, remove_unlisted):

    if not Translation.is_known_event(event):
        return None, None, None, None

    event_name, event_type, event_context, event_code = Translation.translate_to_event(
        event, verbose)

    abstract_event_name = Abstraction.get_abstract_event_name(
        event_name, remove_unlisted)

    if abstract_event_name is None:
        return None, event_name, event_context, event_code 
    elif not verbose:
        return abstract_event_name, event_name, event_context, event_code

    result = event_type

    if event_context is not None and verbose:
        result += (" (" + event_context + " " + event_code + ")")

    if event_name is not None:
        result += (": " + abstract_event_name)

    return result, event_name, event_context, event_code


@timer
def cohort_to_event_log(cohort, trace_type, verbose=False, remove_unlisted=True, event_filter=None, trace_filter=None):
    # get necessary data from cohort

    patients = cohort.get(PatientWithAttributes())
    encounters = cohort.get(EncounterWithVisit())
    events = cohort.get(DiagnosisWithTime(),
                        ProcedureWithTime(), DrugWithTime())

    patient_events = get_patient_events(patients, events)

    if trace_type == "encounter":
        # mrn -> encounter_keys
        patient_encounters = get_patient_encounters(patients, encounters)
        patient_encounter_keys = get_encounter_keys_per_patient(
            patient_encounters)
        # mrn -> encounter_key -> events
        events_per_patient = get_patient_events_per_encounter(
            patients, patient_encounter_keys, patient_events)
    elif trace_type == "visit":
        patient_encounters = get_patient_encounters(patients, encounters)
        # mrn -> encounter_visit_ids -> encounter_key
        patient_visits_and_encounters = get_visits_and_encounters_per_patient(
            patients, patient_encounters)
        # mrn -> encounter_visit_ids -> events
        events_per_patient = get_patient_events_per_visit(
            patients, patient_visits_and_encounters, patient_events)
    elif trace_type == "mrn":
        events_per_patient = get_patient_events_per_patient(
            patients, patient_events)
    else:
        sys.exit("No matching trace type given. Try using encounter, visit, or mrn")

    filtered_events = filter_events(
        events_per_patient, trace_filter=trace_filter)

    log = create_log_from_filtered_events(
        filtered_events, verbose, remove_unlisted, event_filter=event_filter, patients=patients)

    return log


def save_event_log_to_file(log, file_path):
    with open(file_path, "w") as file:
        XesXmlSerializer().serialize(log, file)
