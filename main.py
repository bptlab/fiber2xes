import os
import re
import sys
import functools
import csv
import pandas as pd
import numpy as np
import datetime
import time
import math

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

from .fiberpatch import (
    DiagnosisWithTime,
    DrugWithTime,
    EncounterWithVisit,
    MaterialWithTime,
    PatientWithAttributes,
    ProcedureWithTime
)

from .xesfactory import XESFactory
from .tracetypes import EncounterBasedTraces, VisitBasedTraces, MRNBasedTraces

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

@timer
def cohort_to_event_log(cohort, trace_type, verbose=False, remove_unlisted=True, event_filter=None, trace_filter=None):
    # get necessary data from cohort

    patients = cohort.get(PatientWithAttributes())
    encounters = cohort.get(EncounterWithVisit())
    events = cohort.get(DiagnosisWithTime(),
                        ProcedureWithTime(), DrugWithTime())

    patient_events = get_patient_events(patients, events)

    if trace_type == "encounter":
        events_per_patient = EncounterBasedTraces.get_traces_per_patient(patients, encounter, patient_events)
    elif trace_type == "visit":
        events_per_patient = VisitBasedTraces.get_traces_per_patient(patients, encounter, patient_events)
    elif trace_type == "mrn":
        events_per_patient = MRNBasedTraces.get_traces_per_patient(patients, patient_events)
    else:
        sys.exit("No matching trace type given. Try using encounter, visit, or mrn")

    filtered_events = filter_events(
        events_per_patient, trace_filter=trace_filter)

    log = XESFactory.create_log_from_traces(
        filtered_events,
        verbose,
        remove_unlisted,
        event_filter=event_filter,
        patients=patients
    )

    return log

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
        if tup not in unique_events:
            unique_events.add(tup)
        else:
            indexes_to_drop.append(index)
    patient_events.drop(patient_events.index[indexes_to_drop], inplace=True)
    return patient_events

def timestamp_from_birthdate_and_age_and_time(date, age_in_days, time_of_day_key):
    if math.isnan(age_in_days):
        return date
    else:
        timestamp_without_hours_and_minutes = date + \
            datetime.timedelta(days=age_in_days)
        date_without_time = pd.to_datetime(
            timestamp_without_hours_and_minutes, errors='coerce')
        date_with_time = date_without_time + \
            datetime.timedelta(minutes=time_of_day_key)
        return date_with_time

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
