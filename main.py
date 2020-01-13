import sys
import functools
import pandas as pd
import datetime
import time
import math

from .fiberpatch import (
    DiagnosisWithTime,
    DrugWithTime,
    EncounterWithVisit,
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
        print("Finished "+str(func.__name__)+" in "+str(run_time)+"s")
        return value
    return wrapper_timer


@timer
def cohort_to_event_log(cohort, trace_type, verbose=False, remove_unlisted=True, event_filter=None, trace_filter=None):
    # get necessary data from cohort

    patients = cohort.get(PatientWithAttributes())
    encounter = cohort.get(EncounterWithVisit())
    events = cohort.get(DiagnosisWithTime(),
                        ProcedureWithTime(), DrugWithTime())

    patient_events = get_patient_events(patients, events)

    if trace_type == "encounter":
        traces_per_patient = EncounterBasedTraces.get_traces_per_patient(patients, encounter, patient_events)
    elif trace_type == "visit":
        traces_per_patient = VisitBasedTraces.get_traces_per_patient(patients, encounter, patient_events)
    elif trace_type == "mrn":
        traces_per_patient = MRNBasedTraces.get_traces_per_patient(patients, patient_events)
    else:
        sys.exit("No matching trace type given. Try using encounter, visit, or mrn")

    filtered_traces_per_patient = filter_traces(traces_per_patient, trace_filter=trace_filter)

    log = XESFactory.create_xml_log_from_traces(
        filtered_traces_per_patient,
        verbose,
        remove_unlisted,
        event_filter=event_filter,
        patients=patients
    )

    return log


@timer
def get_patient_events(patients, events):
    # join patients and events
    patient_events = pd.merge(patients, events, on='medical_record_number', how='inner')

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
def filter_traces(traces_to_filter, trace_filter=None):
    filtered_traces_per_patient = {}
    for mrn in traces_to_filter:
        for trace_key in traces_to_filter[mrn]:
            is_relevant = False
            if trace_filter is None:
                is_relevant = True
            else:
                is_relevant = trace_filter.is_relevant_trace(
                    traces_to_filter[mrn][trace_key])

            if is_relevant:
                if mrn not in filtered_traces_per_patient:
                    filtered_traces_per_patient[mrn] = {}
                if trace_key not in filtered_traces_per_patient[mrn]:
                    filtered_traces_per_patient[mrn][trace_key] = {}
                filtered_traces_per_patient[mrn][trace_key] = traces_to_filter[mrn][trace_key]

    return filtered_traces_per_patient
