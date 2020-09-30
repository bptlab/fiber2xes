"""
    Main module of fiber2xes
    It contains functions to create a xes log from events extracted from the MSDW.
"""

from .fiberpatch import (
    DiagnosisWithTime,
    DrugWithTime,
    EncounterWithVisit,
    MetaDataWithOnlyLevels,
    PatientWithAttributes,
    ProcedureWithTime
)
from .xesfactory import create_xes_traces_from_traces
import sys
import functools
import datetime
import time
import math
import multiprocessing
from collections import OrderedDict

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark import SparkConf


from opyenxes.factory.XFactory import XFactory  # type: ignore
from opyenxes.factory.XFactory import XTrace
from opyenxes.factory.XFactory import XLog

from typing import List
from typing import TypeVar
from typing import Tuple

from fiber import Cohort  # type: ignore
from fiber import condition
from fiber import operator

Filter = TypeVar('Filter', condition.Diagnosis, condition.Generic, condition.Material,
                 condition.Procedure, condition.Time, operator.And, operator.Not, operator.Or)


def timer(func):
    """
    Decorator to benchmark functions

    Keyword arguments:
    func - function which should be benchmarked
    """

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
    """
    Creates a spark dataframe from a pandas dataframe

    Keyword arguments:
    spark -- the spark session
    pandas_df -- the pandas dataframe
    """
    pandas_df = define_column_types_for_patient_events(pandas_df)
    return spark.createDataFrame(pandas_df)


@timer
def cohort_to_event_log(cohort: Cohort, trace_type: str, verbose: bool = False, remove_unlisted: bool = True,
                        remove_duplicates: bool = True, event_filter: Filter = None, trace_filter: Filter = None,
                        cores: int = multiprocessing.cpu_count(), window_size: int = 200, abstraction_path: str = None,
                        abstraction_exact_match: bool = False, abstraction_delimiter: str = ";",
                        anamnesis_events: str = 'all'):
    """
    Converts a fiber cohort to an xes event log.
    Therefore it slices the cohort to smaller windows (because of memory restrictions)
    and calls the method `cohort_to_event_log_for_window` with the slices.

    Keyword arguments:
    cohort -- the fiber cohort
    trace_type -- the type of a trace (`mrn` or `visit`)
    verbose -- flag if the events should contain original non abstracted values (default False)
    remove_unlisted -- flag if a trace should only contain listed events (default True)
    remove_duplicates -- flag if duplicate events should be removed (default True)
    event_filter -- a custom filter to filter events (default None)
    trace_filter -- a custom filter to filter traces (default None)
    cores -- the number of cores which should be used to process the cohort (default amount of CPUs)
    window_size -- the number of patients per window (default 500)
    abstraction_path -- the path to the abstraction file (default None)
    abstraction_exact_match -- flag if the abstraction algorithm should only
                               abstract exacted matches (default False)
    abstraction_delimiter -- the delimiter of the abstraction file (default ;)
    anamnesis_events -- define which anamnesis events should be extracted: all,
                        listed or none (default all)
    """

    if trace_type != "visit" and trace_type != "mrn":
        sys.exit("No matching trace type given. Try using visit or mrn.")

    if anamnesis_events != "all" and anamnesis_events != "listed" and anamnesis_events != 'none':
        sys.exit(
            "No matching anamnesis_events value given. Try using all, listed or none")

    manager = multiprocessing.Manager()
    traces: List[XTrace] = manager.list()

    mrns = list(cohort.mrns())
    window_amount = math.ceil(len(mrns)/window_size)

    # Spawn a new process for each window to free memory after each window completion
    for i in range(0, window_amount):
        print("Start window {current_window} / {max_window}".format(
            current_window=(i + 1),
            max_window=window_amount))
        window_start_time = time.perf_counter()
        mrns_in_window = mrns[i * window_size: (i + 1) * window_size]
        cohort_for_window = Cohort(condition.MRNs(mrns_in_window))

        process = multiprocessing.Process(target=cohort_to_event_log_for_window, args=(
            cohort_for_window,
            trace_type,
            verbose,
            remove_unlisted,
            remove_duplicates,
            event_filter,
            trace_filter,
            cores,
            abstraction_path,
            abstraction_exact_match,
            abstraction_delimiter,
            anamnesis_events,
            traces
        ))
        process.start()
        process.join()
        print("Finished window {current_window} / {max_window} in {window_time} s".format(
            current_window=(i + 1),
            max_window=window_amount,
            window_time=(time.perf_counter() - window_start_time)
        ))

    log: XLog = XFactory.create_log()

    for trace in traces:
        log.append(trace)
    return log


def cohort_to_event_log_for_window(cohort, trace_type: str, verbose: bool, remove_unlisted: bool, remove_duplicates: bool,
                                   event_filter: Filter, trace_filter: Filter, cores: int, abstraction_path: str,
                                   abstraction_exact_match: bool, abstraction_delimiter: str,
                                   anamnesis_events: str, traces: List[XTrace]):
    """
    Converts a window of the patient to XES traces and store them in the given `traces` parameter.

    Keyword arguments:
    cohort -- the fiber cohort
    trace_type -- the type of a trace (`mrn` or `visit`)
    verbose -- flag if the events should contain original non abstracted values
    remove_unlisted -- flag if a trace should only contain listed events
    remove_duplicates -- flag if duplicate events should be removed
    event_filter -- a custom filter to filter events
    trace_filter -- a custom filter to filter traces
    cores -- the number of cores which should be used to process the cohort
    abstraction_path -- the path to the abstraction file
    abstraction_exact_match -- flag if the abstraction algorithm should only abstract
                               exacted matches
    abstraction_delimiter -- the delimiter of the abstraction file
    traces -- a container to collect all traces
    """

    # Get necessary data from cohort
    patients = cohort.get(PatientWithAttributes())
    print("Fetched Patients")

    events = cohort.get(DiagnosisWithTime(),
                        ProcedureWithTime(), DrugWithTime())
    print("Fetched Events")

    patient_events_pd = merge_dataframes(
        patients, events, 'medical_record_number')

    del patients
    del events

    encounters = cohort.get(EncounterWithVisit())
    print("Fetched Encouters")
    encounters = encounters.drop(columns=["encounter_type", "encounter_class"])
    patient_events_pd = merge_dataframes(
        patient_events_pd,
        encounters,
        join_columns=["encounter_key", "medical_record_number", "age_in_days"]
    )
    del encounters

    metadata = cohort.get(MetaDataWithOnlyLevels())
    print("Fetched MetaData")
    patient_events_pd = merge_dataframes(
        patient_events_pd,
        metadata,
        join_columns=["meta_data_key"]
    )
    del metadata

    print("Finished dataset preparation")

    # Initialize spark session
    conf = SparkConf()\
        .setAppName("fiber2xes")\
        .set("spark.driver.memory", "90g")\
        .set("spark.memory.fraction", "0.95")\
        .set("spark.memory.storageFraction", "0.95")\
        .set("spark.memory.offHeap.enabled", "true")\
        .set("spark.memory.offHeap.size", "4g")\
        .set("spark.executor.memory", "90g")\
        .set("spark.driver.maxResultSize", "90g")\
        .set("spark.cores.max", str(multiprocessing.cpu_count()))\
        .set("spark.sql.execution.arrow.enabled", "true")\
        .set("spark.sql.shuffle.partitions", "200")\
        .setMaster("local[{cores}]".format(cores=cores))
    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .getOrCreate()

    print("Initialized spark")

    patient_events: DataFrame = create_spark_df(spark, patient_events_pd)

    # Create column mapping to be able to convert rdds back to data frames
    column_indices = OrderedDict(
        zip(
            list(patient_events.schema.names) + ["timestamp"],
            range(0, len(patient_events.schema.names) + 1)
        )
    )

    patient_events_with_timestamp: DataFrame = calculate_timestamp(
        patient_events, column_indices)
    column_indices = OrderedDict(
        zip(
            list(column_indices.keys()) + ["trace_id"],
            list(column_indices.values()) + [len(column_indices)]
        )
    )

    patient_events.unpersist()

    # Generate trace ids for every event according to trace type
    if trace_type == "visit":
        traces_per_patient = get_traces_per_patient_by_visit(
            patient_events_with_timestamp, column_indices)
    else:
        traces_per_patient = get_traces_per_patient_by_mrn(
            patient_events_with_timestamp, column_indices)

    patient_events_with_timestamp.unpersist()

    traces_per_patient = traces_per_patient.repartition("trace_id")

    filtered_traces_per_patient = filter_traces(
        traces_per_patient, trace_filter=trace_filter)

    if trace_type == "visit":
        filtered_traces_per_patient = visit_to_mrn(filtered_traces_per_patient)

    traces_in_window: List[List[XTrace]] = create_xes_traces_from_traces(
        filtered_traces_per_patient,
        abstraction_path=abstraction_path,
        abstraction_exact_match=abstraction_exact_match,
        abstraction_delimiter=abstraction_delimiter,
        verbose=verbose,
        remove_unlisted=remove_unlisted,
        remove_duplicates=remove_duplicates,
        event_filter=event_filter,
        trace_type=trace_type,
        anamnesis_events=anamnesis_events,
    )

    filtered_traces_per_patient.unpersist()
    spark.stop()
    for trace_array in traces_in_window:
        traces += trace_array


def handle_duplicate_column_names(dataframe) -> pd.DataFrame:
    """
    Takes a Pandas DataFrame and renames duplicate columns for later use with Spark.

    Keyword arguments:
    df -- the dataframe
    """
    columns = []
    counter = 0
    for column in dataframe.columns:
        if column in columns:
            while True:
                counter += 1
                new_name = "{column_name}_{counter}".format(
                    column_name=column, counter=counter)
                if new_name not in columns:
                    break
            columns.append(new_name)
        else:
            columns.append(column)
    dataframe.columns = columns
    return dataframe


def define_column_types_for_patient_events(patient_events: pd.DataFrame) -> pd.DataFrame:
    """Redefines the type of columns belonging to the patient_events Pandas DataFrame to strings."""
    patient_events['date_of_birth'] = patient_events['date_of_birth'].astype(
        str)
    patient_events['religion'] = patient_events['religion'].astype(str)
    patient_events['patient_ethnic_group'] = patient_events['patient_ethnic_group'].astype(
        str)
    patient_events['language'] = patient_events.language.astype(str)
    return patient_events


@timer
def merge_dataframes(left: pd.DataFrame, right: pd.DataFrame, join_columns: List[str]) -> pd.DataFrame:
    """
    Merges two Pandas DataFrames with an inner join on a given column and frees the
    original frames from memory.

    Keyword arguments:
    left - one dataframe to join
    right - one dataframe to join
    join_column - attribute which should be join condition
    """
    deduplicated_left: pd.DataFrame = handle_duplicate_column_names(left)
    deduplicated_right: pd.DataFrame = handle_duplicate_column_names(right)
    result = pd.merge(left=deduplicated_left, right=deduplicated_right,
                      on=join_columns, how='inner')
    del left
    del right
    return result


@timer
def calculate_timestamp(patient_events: DataFrame, column_indices: OrderedDict) -> DataFrame:
    """
    Calculates the timestamp for all patient_events based on a patient's `date_of_birth`,
    `age_in_days` and the specific `time_of_day`

    Keyword arguments:
    patient_events -- A Spark DataFrame containing all patient's events
    column_indices -- A dictionary containing a mapping from column names to their indices for
                      the DataFrame
    """
    return patient_events\
        .filter('not isnan(age_in_days) and date_of_birth <> "MSDW_UNKNOWN"')\
        .rdd\
        .map(lambda row: row + timestamp_from_birthdate_and_age_and_time(
            row[column_indices["date_of_birth"]],
            row[column_indices["age_in_days"]],
            row[column_indices["time_of_day_key"]]))\
        .distinct()\
        .toDF(list(column_indices.keys()))


def timestamp_from_birthdate_and_age_and_time(date,
                                              age_in_days: int,
                                              time_of_day: int) -> Tuple[datetime.datetime, ]:
    """
    Calculates a single timestamp

    Keyword arguments:
    date - birthdate of the patient
    age_in_days - The age of the patient in days
    time_of_day - The time of the day
    """
    time_info = date.split("-")
    date = datetime.datetime(int(time_info[0]), int(
        time_info[1]), int(time_info[2]))
    timestamp = date + \
        datetime.timedelta(days=age_in_days) + \
        datetime.timedelta(minutes=time_of_day)
    return (timestamp, )


@timer
def filter_traces(traces_to_filter: DataFrame, trace_filter: Filter) -> RDD:
    """
    Filters out traces that do not match the specified trace filter

    Keyword arguments:
    traces_to_filter -- the trace list
    trace_filter -- the trace filter (default None)
    """

    if trace_filter is None:
        return traces_to_filter.rdd

    return traces_to_filter\
        .rdd\
        .map(lambda row: (row.trace_id, row))\
        .combineByKey(create_list, add_tuple_to_list, merge_lists)\
        .filter(lambda trace: trace_filter.is_relevant_trace(trace[1]))


def get_traces_per_patient_by_mrn(patient_events: DataFrame, column_indices: OrderedDict):
    """
    Generate traces according to medical record number

    Keyword arguments:
    patient_events - All events of a patient
    column_indices - Column mapping
    """
    return patient_events\
        .rdd\
        .map(lambda row: row + (row[column_indices["medical_record_number"]], ))\
        .toDF(list(patient_events.schema.names) + ["trace_id"])


def get_traces_per_patient_by_visit(patient_event_encounters: DataFrame, column_indices: OrderedDict) -> DataFrame:
    """
    Generate traces according to encounter visit id

    Keyword arguments:
    patient_events - All events of a patient
    column_indices - Column mapping
    """
    return patient_event_encounters\
        .rdd\
        .map(lambda row: row + (row[column_indices["encounter_visit_id"]], ))\
        .toDF(list(patient_event_encounters.schema.names) + ["trace_id"])


def visit_to_mrn(visit_traces: RDD) -> RDD:
    """
    transform visit based traces to mrn based traces

    visit_traces - all traces with encounter visit id as trace id
    """

    return visit_traces\
        .map(lambda trace: (trace[1][0].medical_record_number, trace[1]))\
        .combineByKey(return_object, merge_lists, merge_lists)


def create_list(object_to_list: object) -> list:
    """
    Create a list containing the passed object

    Keyword arguments:
    object_to_list - the object which should be returned as List
    """
    return [object_to_list]


def return_object(object_to_return: object) -> object:
    """
    Simply return the object for combineByKey()

    Keyword arguments:
    object_to_return - the object which should be returned
    """
    return object_to_return


def merge_lists(list_to_merge1: list, list_to_merge2: list) -> list:
    """
    Merge two lists

    Keyword arguments:
    list_to_merge1 - list to merge
    list_to_merge2 - list to merge
    """
    return list_to_merge1 + list_to_merge2


def add_tuple_to_list(present_list: list, new_element: object) -> list:
    """
    Add an element to an existing list

    Keyword arguments:
    present_list - Existing list
    new_element - element which should be added to the list
    """
    return present_list + [new_element]
