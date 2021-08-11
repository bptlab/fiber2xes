import logging
from collections import OrderedDict


import pandas as pd
from pandas import DataFrame

#from pandas.util.testing import assert_frame_equal
import pytest

from tempfile import NamedTemporaryFile

from chispa.dataframe_comparer import assert_df_equality  # type: ignore

from opyenxes.factory.XFactory import XTrace  # type: ignore
from opyenxes.factory.XFactory import XFactory  # type: ignore
from opyenxes.factory.XFactory import XLog

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import (
    TimestampType, StringType, LongType)

from fiber2xes.fiber2xes import (create_xes_traces_from_traces)

from fiber2xes.helper import (get_traces_per_patient_by_visit,
                              get_traces_per_patient_by_mrn, filter_traces,
                              visit_to_mrn,
                              create_spark_df, calculate_timestamp,
                              merge_dataframes)


from fiber2xes.filter.condition import (
    Diagnosis,
    Time
)

from fiber2xes.xesfactory.xesfactory import (
    save_event_log_to_file, save_event_log_to_temporary_file)

from fiber2xes.filter.operator import And


@pytest.fixture(scope="session")
def spark_context(request) -> SparkContext:
    """ fixture for creating a spark context
    Args:
        request: pytest.FixtureRequest object
    """
    conf = (SparkConf().setMaster("local[1]").setAppName(
        "pytest-pyspark-local-testing"))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())

    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)
    return sc


@pytest.fixture(scope="session")
def sql_context(spark_context) -> SQLContext:
    sc = SQLContext(spark_context)
    return sc


def test_patient_events():
    patient_df = pd.read_csv("test/e2e/mock_data/patients.csv")
    events_df = pd.read_csv("test/e2e/mock_data/events.csv")

    merged_patient_events = merge_dataframes(
        patient_df, events_df, "medical_record_number")

    expected_result = pd.read_csv("test/e2e/mock_data/patient_events_pd.csv")
    pd.testing.assert_frame_equal(merged_patient_events, expected_result)


def test_encounters():
    encounters = pd.read_csv("test/e2e/mock_data/encounters.csv").drop(
        columns=["encounter_type", "encounter_class", "age_in_days"])

    patient_events_pd = pd.read_csv("test/e2e/mock_data/patient_events_pd.csv")

    merged_encounter_events = merge_dataframes(
        patient_events_pd,
        encounters,
        ["encounter_key", "medical_record_number"]
    )

    expected_result = pd.read_csv("test/e2e/mock_data/patient_events_pd_2.csv")
    pd.testing.assert_frame_equal(merged_encounter_events, expected_result)


def test_metadata():
    metadata = pd.read_csv("test/e2e/mock_data/metadata.csv")
    patient_events_pd = pd.read_csv(
        "test/e2e/mock_data/patient_events_pd_2.csv")

    merged_events_metadata = merge_dataframes(
        patient_events_pd,
        metadata,
        join_columns=["meta_data_key"]
    )

    expected_result = pd.read_csv("test/e2e/mock_data/patient_events_pd_3.csv")
    pd.testing.assert_frame_equal(merged_events_metadata, expected_result)


@pytest.mark.usefixtures("sql_context")
def test_spark_df_creation(sql_context: SQLContext):
    pandas_df = pd.read_csv("test/e2e/mock_data/patient_events_pd_3.csv")

    pandas_df = pandas_df.replace({'None': None}) # type: ignore
    pandas_df = pandas_df.applymap(str) # type: ignore

    patient_events_df = create_spark_df(sql_context, pandas_df)
    patient_events_df = patient_events_df.replace('nan', None)

    expected_df = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/patient_events_df.csv")

    assert_df_equality(patient_events_df, expected_df)


@pytest.mark.usefixtures("sql_context")
def test_spark_timestamp_calculation(sql_context: SQLContext):
    patient_events = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/patient_events_df.csv")

    patient_events = change_long_datatypes(patient_events)

    column_indices = OrderedDict(
        zip(
            list(patient_events.schema.names) + ["timestamp"],
            range(0, len(patient_events.schema.names) + 1)
        )
    )

    patient_events_with_timestamp: DataFrame = calculate_timestamp(
        patient_events, column_indices)

    patient_events_with_timestamp = change_additional_datatypes(
        patient_events_with_timestamp)

    expected_df = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/patient_events_df_withTimestamp.csv")

    expected_df = expected_df.replace('nan', None)
    expected_df = change_timestamp_datatype(expected_df)
    expected_df = change_additional_datatypes(expected_df)
    expected_df = change_long_datatypes(expected_df)

    assert_df_equality(patient_events_with_timestamp, expected_df)


@pytest.mark.usefixtures("sql_context")
def test_trace_id_generation(sql_context: SQLContext):
    timestamp_df = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/patient_events_df_withTimestamp.csv")

    timestamp_df = timestamp_df.replace('nan', None)

    timestamp_df = change_timestamp_datatype(timestamp_df)
    timestamp_df = change_long_datatypes(timestamp_df)

    column_indices = OrderedDict(
        zip(
            list(timestamp_df.schema.names) + ["trace_id"],
            range(0, len(timestamp_df.schema.names) + 1)
        )
    )

    visit_traces = get_traces_per_patient_by_visit(
        timestamp_df, column_indices)

    visit_traces = change_additional_datatypes(visit_traces)

    mrn_traces = get_traces_per_patient_by_mrn(
        timestamp_df, column_indices
    )

    mrn_traces = change_additional_datatypes(mrn_traces)

    assert visit_traces != mrn_traces

    expected_visit_df = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/visit_traces.csv")

    expected_visit_df = expected_visit_df.replace('nan', None)
    expected_visit_df = change_timestamp_datatype(expected_visit_df)
    expected_visit_df = change_additional_datatypes(expected_visit_df)
    expected_visit_df = change_long_datatypes(expected_visit_df)

    expected_mrn_df = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/mrn_traces.csv")

    expected_mrn_df = expected_mrn_df.replace('nan', None)
    expected_mrn_df = change_timestamp_datatype(expected_mrn_df)
    expected_mrn_df = change_additional_datatypes(expected_mrn_df)
    expected_mrn_df = change_long_datatypes(expected_mrn_df)

    assert_df_equality(expected_visit_df, visit_traces)
    assert_df_equality(expected_mrn_df, mrn_traces)

# test fiber2xes.py:279


@pytest.mark.usefixtures("sql_context")
def test_trace_filter(sql_context: SQLContext):

    patient_traces = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/mrn_traces.csv")

    all_before = Time(all_events_before="2070-01-20 00:00:00")
    diagnosis = Diagnosis("M54.5")
    trace_filter = And(all_before, diagnosis)

    patient_traces = patient_traces.replace('nan', None)
    patient_traces = change_timestamp_datatype(patient_traces)
    patient_traces = change_long_datatypes(patient_traces)
    patient_traces = change_datatypes(patient_traces)

    filtered_traces_per_patient = filter_traces(
        patient_traces, trace_filter=trace_filter)

    visit_traces = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/visit_traces.csv")

    visit_traces = visit_traces.replace('nan', None)
    visit_traces = change_datatypes(visit_traces)
    visit_traces = change_timestamp_datatype(visit_traces)
    visit_traces = change_long_datatypes(visit_traces)

    filtered_traces_per_visit = filter_traces(
        visit_traces, trace_filter=trace_filter)

    visit_df = filtered_traces_per_visit.toDF(sampleRatio=1)
    mrn_df = filtered_traces_per_patient.toDF(sampleRatio=1)

    assert mrn_df != visit_df
    assert mrn_df.count() == 1

    assert visit_df.count() == 2

    visit_traces_with_mrn_key = visit_to_mrn(filtered_traces_per_visit)
    visit_mrn_df = visit_traces_with_mrn_key.toDF(sampleRatio=1)

    assert mrn_df != visit_mrn_df
    assert visit_df != visit_mrn_df

    assert visit_mrn_df.count() == 1

# test fiber2xes.py 292


@pytest.mark.usefixtures("sql_context")
def test_visit_trace_creation(sql_context: SQLContext):
    timestamp_df = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/patient_events_df_withTimestamp.csv")

    timestamp_df = timestamp_df.replace('nan', None)

    timestamp_df = change_timestamp_datatype(timestamp_df)
    timestamp_df = change_long_datatypes(timestamp_df)

    column_indices = OrderedDict(
        zip(
            list(timestamp_df.schema.names) + ["trace_id"],
            range(0, len(timestamp_df.schema.names) + 1)
        )
    )

    visit_traces = get_traces_per_patient_by_visit(
        timestamp_df, column_indices)

    visit_traces = change_datatypes(visit_traces)

    event_filter = Diagnosis("M54.5")

    visit_traces = visit_traces.replace('nan', None)
    visit_traces = change_datatypes(visit_traces)
    visit_traces = change_timestamp_datatype(visit_traces)
    visit_traces = change_long_datatypes(visit_traces)

    visit_traces.repartition("trace_id")

    filtered_traces_per_visit = filter_traces(
        visit_traces, trace_filter=None)
    tracesss = visit_to_mrn(filtered_traces_per_visit)
    trace_lists = create_xes_traces_from_traces(
        tracesss, abstraction_path="./abstraction-tables/abstraction_high_level.csv", abstraction_exact_match=True,
        abstraction_delimiter=",", verbose=True, remove_unlisted=False, event_filter=event_filter, remove_duplicates=False, trace_type="visit",
        include_anamnesis_events=True,  perform_complex_duplicate_detection=False, duplicate_event_identifier="BACK PAIN",
        event_identifier_to_merge="CHRONIC LOW BACK PAIN")

    assert trace_lists is not None
    log: XLog = XFactory.create_log()

    for trace_list in trace_lists:
        for trace in trace_list:
            assert type(trace) == XTrace
            case_ID = trace.get_attributes().get(
                "concept:name").get_value()
            assert case_ID in ["1", "2", "3", "4"]
            log.append(trace)

    temp_visit_log_file = NamedTemporaryFile()
    save_event_log_to_temporary_file(log, temp_visit_log_file)

    with open(temp_visit_log_file.name) as test_log, open('./test/e2e/mock_data/reference_visit_log.xes') as reference_log:
        filtered_test_log = filter(ignore_uuid_lines, test_log)
        filtered_reference_log = filter(ignore_uuid_lines, reference_log)
        assert(all(x == y for x, y in zip(
            filtered_test_log, filtered_reference_log)))
        reference_log.close()
        test_log.close()
    


@pytest.mark.usefixtures("sql_context")
def test_mrn_creation(sql_context: SQLContext):
    timestamp_df = sql_context.read.options(header='True', inferSchema=False, delimiter=',').csv(
        "test/e2e/mock_data/patient_events_df_withTimestamp.csv")

    timestamp_df = timestamp_df.replace('nan', None)

    timestamp_df = change_timestamp_datatype(timestamp_df)
    timestamp_df = change_long_datatypes(timestamp_df)

    column_indices = OrderedDict(
        zip(
            list(timestamp_df.schema.names) + ["trace_id"],
            range(0, len(timestamp_df.schema.names) + 1)
        )
    )

    patient_traces = get_traces_per_patient_by_mrn(
        timestamp_df, column_indices
    )

    patient_traces = change_datatypes(patient_traces)

    event_filter = Diagnosis("M54.5")

    patient_traces = patient_traces.replace('nan', None)
    patient_traces = change_timestamp_datatype(patient_traces)
    patient_traces = change_long_datatypes(patient_traces)
    patient_traces = change_datatypes(patient_traces)

    filtered_traces_per_patient = filter_traces(
        patient_traces, trace_filter=None)
    tracesss = visit_to_mrn(filtered_traces_per_patient)
    trace_lists = create_xes_traces_from_traces(
        tracesss, abstraction_path="./abstraction-tables/abstraction_high_level.csv", abstraction_exact_match=True,
        abstraction_delimiter=",", verbose=True, remove_unlisted=False, event_filter=event_filter, remove_duplicates=False, trace_type="mrn",
        include_anamnesis_events=True, perform_complex_duplicate_detection=False, duplicate_event_identifier="BACK PAIN",
        event_identifier_to_merge="CHRONIC LOW BACK PAIN")

    assert trace_lists is not None
    log: XLog = XFactory.create_log()

    for trace_list in trace_lists:
        for trace in trace_list:
            assert type(trace) == XTrace
            case_ID = trace.get_attributes().get(
                "concept:name").get_value()
            assert case_ID in ["1", "2"]
            log.append(trace)

    temp_mrn_log_file = NamedTemporaryFile()
    save_event_log_to_temporary_file(log, temp_mrn_log_file)

    with open(temp_mrn_log_file.name) as test_log, open('./test/e2e/mock_data/reference_mrn_log.xes') as reference_log:
        filtered_test_log = filter(ignore_uuid_lines, test_log)
        filtered_reference_log = filter(ignore_uuid_lines, reference_log)
        assert(all(x == y for x, y in zip(
            filtered_test_log, filtered_reference_log)))
        reference_log.close()
        test_log.close()
   


def change_timestamp_datatype(df):
    df = df.withColumn(
        "timestamp", df["timestamp"].cast(TimestampType()))
    return df


def change_long_datatypes(df):
    df = df.withColumn(
        "age_in_days", df["age_in_days"].cast(LongType()))
    df = df.withColumn(
        "time_of_day_key", df["time_of_day_key"].cast(LongType()))
    return df


def change_datatypes(df):
    df = df.withColumn(
        "deceased_indicator", df["deceased_indicator"].cast(StringType()))
    df = df.withColumn(
        "mother_account_number", df["mother_account_number"].cast(StringType()))
    df = df.withColumn(
        "language", df["language"].cast(StringType()))
    df = df.withColumn(
        "citizenship", df["citizenship"].cast(StringType()))
    return df


def change_additional_datatypes(df):
    df = change_datatypes(df)
    df = df.withColumn(
        "value", df["value"].cast(StringType()))
    df = df.withColumn(
        "procedure_control_key", df["procedure_control_key"].cast(StringType()))
    return df


def ignore_uuid_lines(line):
    """
    A filter predicate that can be used to ignore IDs when comparing log files,
    as they are created randomly
    """

    if ('<id key="id" value=' in line) or ('<log ' in line) or ('<!--' in line):
        return False  # ignore lines that can differ due to indeterminism
    return True
