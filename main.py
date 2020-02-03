import sys
import functools
import pandas as pd
import datetime
import time
import math
import multiprocessing
from collections import OrderedDict

from .fiberpatch import (
    DiagnosisWithTime,
    DrugWithTime,
    EncounterWithVisit,
    PatientWithAttributes,
    ProcedureWithTime
)

from .xesfactory import create_xes_log_from_traces
from .tracetypes import get_traces_per_patient_by_mrn, get_traces_per_patient_by_visit

from pyspark.sql import Row, SparkSession
from collections import OrderedDict
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import lit, isnan

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
def create_spark_df(spark, pandas_df):
    pandas_df = define_column_types_for_patient_events(pandas_df)
    return spark.createDataFrame(pandas_df)

@timer
def cohort_to_event_log(cohort, trace_type, verbose=False, remove_unlisted=True, event_filter=None, trace_filter=None):
    # get necessary data from cohort
    patients = cohort.get(PatientWithAttributes())
    events = cohort.get(DiagnosisWithTime(),
                        ProcedureWithTime(), DrugWithTime())

    # Initialize spark session
    conf = SparkConf()\
        .setAppName("fiber2xes")\
        .set("spark.driver.memory", "60g")\
        .set("spark.memory.fraction", "0.95")\
        .set("spark.memory.storageFraction", "0.95")\
        .set("spark.memory.offHeap.enabled", "true")\
        .set("spark.memory.offHeap.size", "4g")\
        .set("spark.executor.memory", "60g")\
        .set("spark.driver.maxResultSize", "60g")\
        .set("spark.cores.max", multiprocessing.cpu_count())\
        .set("spark.sql.execution.arrow.enabled", "true")\
        .setMaster("local[{cores}]".format(cores=multiprocessing.cpu_count()))
    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .getOrCreate()

    patient_events = merge_dataframes(
        patients, events, 'medical_record_number')

    del(patients)
    del(events)

    if trace_type == "visit" or trace_type == "encounter":
        encounters = cohort.get(EncounterWithVisit())
        encounters = encounters.drop(columns=["encounter_type", "encounter_class", "age_in_days"])
        patient_events = merge_dataframes(patient_events, encounters, on=["encounter_key", "medical_record_number"])
        del(encounters)

    patient_events = create_spark_df(spark, patient_events)

    column_indices = OrderedDict(zip(list(patient_events.schema.names) + ["timestamp"], range(0, len(patient_events.schema.names) + 1)))

    patient_events = calculate_timestamp(patient_events, column_indices)

    column_indices = OrderedDict(zip(list(column_indices.keys()) + ["trace_id"], list(column_indices.values()) + [len(column_indices)]))

    if trace_type == "visit":
        traces_per_patient = get_traces_per_patient_by_visit(
            patient_events, column_indices)
    elif trace_type == "mrn":
        traces_per_patient = get_traces_per_patient_by_mrn(
            patient_events, column_indices)
    # todo: add encounter?
    else:
        sys.exit("No matching trace type given. Try using encounter, visit, or mrn")

    filtered_traces_per_patient = filter_traces(
        traces_per_patient, trace_filter=trace_filter)


    log = create_xes_log_from_traces(
        filtered_traces_per_patient,
        verbose,
        remove_unlisted,
        event_filter=event_filter,
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
    result = pd.merge(left, right, on=on, how='inner')
    del(left)
    del(right)
    return result

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
    return (timestamp, )

def createList(a): return [a]
def mergeLists(a, b): return a + b
def addTupleToList(a, b): return a + [b]
    

@timer
def filter_traces(traces_to_filter, trace_filter=None):
    if trace_filter is None:
        return traces_to_filter
    
    return traces_to_filter\
        .rdd\
        .map(lambda row: (row.trace_id, row))\
        .combineByKey(createList, addTupleToList, mergeLists)\
        .filter(lambda trace: trace_filter.is_relevant_trace(trace[1]))
