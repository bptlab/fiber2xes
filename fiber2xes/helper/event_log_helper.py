"""
Provides functionality for the creation and filtering of event log-related data
"""

import datetime
import math
import os

from typing import Tuple
from collections import OrderedDict
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
from fiber2xes.filter.Filter import Filter

from .helper import timer
from .list_helper import (
    create_list, add_tuple_to_list, merge_lists, return_object)


# During tests, this needs to be set to 1, otherwise None works in production.
spark_sampling = os.environ.get('SPARK_SAMPLING_RATE', None)
spark_sampling_rate = float(
    spark_sampling) if spark_sampling is not None else None


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
        .toDF(list(column_indices.keys()), spark_sampling_rate)


def timestamp_from_birthdate_and_age_and_time(date,
                                              age_in_days: int,
                                              time_of_day: int) -> Tuple[datetime.datetime, ]:
    """
    Calculates a single timestamp.
    If time_of_day happens to be none, 0 minutes (-> midnight will be used instead)

    Keyword arguments:
    date - birthdate of the patient
    age_in_days - The age of the patient in days
    time_of_day - The time of the day
    """
    time_info = date.split("-")
    date = datetime.datetime(int(time_info[0]), int(
        time_info[1]), int(time_info[2]))
    if time_of_day is None or math.isnan(time_of_day):
        minutes = 0
    else:
        minutes = time_of_day
    timestamp = date + \
        datetime.timedelta(days=age_in_days) + \
        datetime.timedelta(minutes=minutes)
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
        return traces_to_filter\
            .rdd\
            .map(lambda row: (row.trace_id, row))\
            .combineByKey(create_list, add_tuple_to_list, merge_lists)

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
        .toDF(list(patient_events.schema.names) + ["trace_id"], spark_sampling_rate)


def get_traces_per_patient_by_visit(patient_event_encounters: DataFrame,
                                    column_indices: OrderedDict) -> DataFrame:
    """
    Generate traces according to encounter visit id

    Keyword arguments:
    patient_events - All events of a patient
    column_indices - Column mapping
    """
    return patient_event_encounters\
        .rdd\
        .map(lambda row: row + (row[column_indices["encounter_visit_id"]], ))\
        .toDF(list(patient_event_encounters.schema.names) + ["trace_id"], spark_sampling_rate)


def visit_to_mrn(visit_traces: RDD) -> RDD:
    """
    transform visit based traces to mrn based traces

    visit_traces - all traces with encounter visit id as trace id
    """

    return visit_traces\
        .map(lambda trace: (trace[1][0].medical_record_number, trace[1]))\
        .combineByKey(return_object, merge_lists, merge_lists)
