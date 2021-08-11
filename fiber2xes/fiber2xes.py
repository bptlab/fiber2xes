"""
    Main module of fiber2xes
    It contains functions to create a xes log from events extracted from the MSDW.
"""
import os
import sys
import time

import math
import multiprocessing
from collections import OrderedDict
from typing import List

from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkConf
from opyenxes.factory.XFactory import XFactory  # type: ignore
from opyenxes.factory.XFactory import XTrace
from opyenxes.factory.XFactory import XLog

from fiber import Cohort  # type: ignore
from fiber.condition import MRNs  # type: ignore

from fiber2xes.filter.Filter import Filter
from fiber2xes import helper

from .fiberpatch import (
    DiagnosisWithTime,
    DrugWithTime,
    EncounterWithVisit,
    MetaDataWithOnlyLevels,
    PatientWithAttributes,
    ProcedureWithTime
)
from .xesfactory import create_xes_traces_from_traces


@helper.timer
def cohort_to_event_log(cohort: Cohort, trace_type: str,
                        verbose: bool = False,
                        remove_unlisted: bool = True,
                        remove_duplicates: bool = True,
                        event_filter: Filter = None,
                        trace_filter: Filter = None,
                        cores: int = multiprocessing.cpu_count(),
                        window_size: int = 200,
                        abstraction_path: str = None,
                        abstraction_exact_match: bool = False,
                        abstraction_delimiter: str = ";",
                        include_anamnesis_events: bool = True,
                        duplicate_event_identifier: str = "BACK PAIN",
                        event_identifier_to_merge: str = "CHRONIC LOW BACK PAIN",
                        perform_complex_duplicate_detection: bool = False) -> XLog:
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
    include_anamnesis_events -- define whether anamnesis events should be extracted (default True)
    duplicate_event_identifier -- event identifier to be analysed separately for duplications
    event_identifier_to_merge -- event identifier to be used for separately identified duplicates
    perform_complex_duplicate_detection -- should complex duplicate detection be done(default False)
    """

    if trace_type not in ('visit', 'mrn'):
        sys.exit("No matching trace type given. Try using visit or mrn.")

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
        cohort_for_window = Cohort(MRNs(mrns_in_window))

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
            include_anamnesis_events,
            traces,
            duplicate_event_identifier,
            event_identifier_to_merge,
            perform_complex_duplicate_detection
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


def cohort_to_event_log_for_window(cohort, trace_type: str, verbose: bool,
                                   remove_unlisted: bool, remove_duplicates: bool,
                                   event_filter: Filter, trace_filter: Filter,
                                   cores: int, abstraction_path: str,
                                   abstraction_exact_match: bool, abstraction_delimiter: str,
                                   include_anamnesis_events: bool, traces: List[XTrace],
                                   duplicate_event_identifier: str,
                                   event_identifier_to_merge: str,
                                   perform_complex_duplicate_detection: bool = False,):
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
    perform_complex_duplicate_detection -- whether to perform complex, day-based duplicate detection
    """

    # Get necessary data from cohort
    patients = cohort.get(PatientWithAttributes())
    print("Fetched Patients")

    diagnoses = cohort.get(DiagnosisWithTime())
    procedures = cohort.get(ProcedureWithTime())
    drugs = cohort.get(DrugWithTime())

    events = diagnoses.append(procedures)
    events = events.append(drugs)

    del diagnoses
    del procedures
    del drugs

    print("Fetched Events")

    patient_events_pd = helper.merge_dataframes(
        patients, events, 'medical_record_number')

    del patients
    del events

    encounters = cohort.get(EncounterWithVisit())
    print("Fetched Encouters")
    encounters = encounters.drop(columns=["encounter_type", "encounter_class"])
    patient_events_pd = helper.merge_dataframes(
        patient_events_pd,
        encounters,
        join_columns=["encounter_key", "medical_record_number", "age_in_days"]
    )
    del encounters

    metadata = cohort.get(MetaDataWithOnlyLevels())
    print("Fetched MetaData")
    patient_events_pd = helper.merge_dataframes(
        patient_events_pd,
        metadata,
        join_columns=["meta_data_key"]
    )
    del metadata

    print("Finished dataset preparation")

    # load env variables
    load_dotenv(os.path.abspath('..'))

    # Initialize spark session
    conf = SparkConf()\
        .setAppName("fiber2xes")\
        .set("spark.driver.memory", os.getenv("DRIVER_MEMORY", "90g"))\
        .set("spark.memory.fraction", os.getenv("MEMORY_FRACTION", "0.95"))\
        .set("spark.memory.storageFraction", os.getenv("MEMORY_STORAGE_FRACTION", "0.95"))\
        .set("spark.memory.offHeap.enabled", os.getenv("MEMORY_OFF_HEAP_ENABLED", "true"))\
        .set("spark.memory.offHeap.size", os.getenv("MEMORY_OFF_HEAP_SIZE", "4g"))\
        .set("spark.executor.memory", os.getenv("EXECUTOR_MEMORY", "90g"))\
        .set("spark.driver.maxResultSize", os.getenv("DRIVER_MAX_RESULT_SIZE", "90g"))\
        .set("spark.cores.max", str(multiprocessing.cpu_count()))\
        .set("spark.sql.execution.arrow.enabled", os.getenv("SQL_EXECUTION_ARROW_ENABLED", "true"))\
        .set("spark.sql.shuffle.partitions", os.getenv("SQL_SHUFFLE_PARTITIONS", "200"))\
        .setMaster("local[{cores}]".format(cores=cores))
    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .getOrCreate()

    print("Initialized spark")

    patient_events: DataFrame = helper.create_spark_df(
        spark, patient_events_pd, schema=helper.EVENT_DF_SCHEMA)

    # Create column mapping to be able to convert rdds back to data frames
    column_indices = OrderedDict(
        zip(
            list(patient_events.schema.names) + ["timestamp"],
            range(0, len(patient_events.schema.names) + 1)
        )
    )

    patient_events_with_timestamp: DataFrame = helper.calculate_timestamp(
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
        traces_per_patient = helper.get_traces_per_patient_by_visit(
            patient_events_with_timestamp, column_indices)
    else:
        traces_per_patient = helper.get_traces_per_patient_by_mrn(
            patient_events_with_timestamp, column_indices)

    patient_events_with_timestamp.unpersist()

    traces_per_patient = traces_per_patient.replace('nan', None)
    traces_per_patient = traces_per_patient.replace('NaN', None)

    traces_per_patient = traces_per_patient.repartition("trace_id")

    filtered_traces_per_patient = helper.filter_traces(
        traces_per_patient, trace_filter=trace_filter)

    if trace_type == "visit":
        filtered_traces_per_patient = helper.visit_to_mrn(
            filtered_traces_per_patient)

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
        include_anamnesis_events=include_anamnesis_events,
        perform_complex_duplicate_detection=perform_complex_duplicate_detection,
        duplicate_event_identifier=duplicate_event_identifier,
        event_identifier_to_merge=event_identifier_to_merge
    )

    filtered_traces_per_patient.unpersist()
    spark.stop()
    for trace_array in traces_in_window:
        traces += trace_array
