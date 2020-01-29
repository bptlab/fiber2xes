import sys
import functools
import pandas as pd
import datetime
import time
import math
from collections import OrderedDict

from .fiberpatch import (
    DiagnosisWithTime,
    DrugWithTime,
    EncounterWithVisit,
    PatientWithAttributes,
    ProcedureWithTime
)

from .xesfactory import XESFactory
from .tracetypes import get_traces_per_patient_by_mrn, get_traces_per_patient_by_visit

from pyspark.sql import Row, SparkSession
from collections import OrderedDict
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import lit, isnan
import multiprocessing

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
    encounters = cohort.get(EncounterWithVisit())
    events = cohort.get(DiagnosisWithTime(),
                        ProcedureWithTime(), DrugWithTime())

    # Initialize spark session
    conf = SparkConf()\
        .setAppName("fiber2xes")\
        .set("spark.driver.memory", "32g")\
        .set("spark.executor.memory", "32g")\
        .set("spark.driver.maxResultSize", "48g")\
        .set("spark.cores.max", multiprocessing.cpu_count())\
        .set("spark.sql.execution.arrow.enabled", "true")\
        .setMaster("local[{cores}]".format(cores=multiprocessing.cpu_count()))
    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .getOrCreate()

    patient_events = merge_dataframes(
        patients, events, 'medical_record_number')

    if trace_type == "visit":
        patient_events = merge_dataframes(patient_events, encounters, "encounter_key")

    patient_events = define_column_types_for_patient_events(patient_events)

    patient_events = spark.createDataFrame(patient_events)

    column_indices = OrderedDict(zip(list(patient_events.schema.names) + ["timestamp"], range(0, len(patient_events.schema.names) + 1)))

    patient_events = calculate_timestamp(patient_events, column_indices)

    column_indices = OrderedDict(zip(list(column_indices.keys()) + ["trace_id"], list(column_indices.values()) + [len(column_indices)]))

    if trace_type == "visit":
        traces_per_patient = get_traces_per_patient_by_visit(
            patient_events, column_indices)
    elif trace_type == "mrn":
        traces_per_patient = get_traces_per_patient_by_mrn(
            patient_events, column_indices)
    else:
        sys.exit("No matching trace type given. Try using encounter, visit, or mrn")
    # todo: add encounter?
    column_indices["trace_type"] = len(column_indices)

    traces_per_patient.collect()

    return

    filtered_traces_per_patient = filter_traces(
        traces_per_patient, trace_filter=trace_filter)

    log = XESFactory.create_xes_log_from_traces(
        filtered_traces_per_patient,
        verbose,
        remove_unlisted,
        event_filter=event_filter,
        patients=patients
    )

    return log

def handle_duplicate_column_names(df) -> pd.DataFrame:
    columns = []
    counter = 0
    for column in df.columns:
        if column in columns:
            while True:
                counter += 1
                new_name = "{column_name}_{counter}".format(column_name=column, counter=counter)
                if not new_name in columns:
                    break
            columns.append(new_name)
        else:
            columns.append(column)
    df.columns = columns
    return df

def define_column_types_for_patient_events(patient_events) -> pd.DataFrame:
    patient_events.date_of_birth = patient_events.date_of_birth.astype('str')
    patient_events.religion = patient_events.religion.astype('str')
    patient_events.patient_ethnic_group = patient_events.patient_ethnic_group.astype('str')
    patient_events.language = patient_events.language.astype('str')
    return patient_events

@timer            
def merge_dataframes(left, right, on) -> pd.DataFrame:
    left = handle_duplicate_column_names(left)
    right = handle_duplicate_column_names(right)
    return pd.merge(left, right, on=on, how='inner')

@timer
def calculate_timestamp(patient_events, column_indices):
    return patient_events\
        .filter('not isnan(age_in_days) and date_of_birth <> "MSDW_UNKNOWN"')\
        .rdd\
        .map(lambda row: row + timestamp_from_birthdate_and_age_and_time(
            row[column_indices["date_of_birth"]], row[column_indices["age_in_days"]], row[column_indices["time_of_day_key"]]))\
        .distinct()\
        .toDF(list(column_indices.keys()))

def timestamp_from_birthdate_and_age_and_time(date, age_in_days, time_of_day_key) -> (datetime.datetime, ):
    time_info = date.split("-")
    date = datetime.datetime(int(time_info[0]), int(time_info[1]), int(time_info[2]))
    timestamp = date + \
        datetime.timedelta(days=age_in_days) + \
        datetime.timedelta(minutes=time_of_day_key)
    return (timestamp, ) # .strftime("%Y-%m-%d %H:%M"), ) <-- for string representation

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
