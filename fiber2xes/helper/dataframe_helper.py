"""
Provides functionality for handling and manipulating data frames
"""

import pandas as pd
from pyspark.sql.types import (
    StringType, StructType, StructField, IntegerType, DoubleType)

from .helper import timer


EVENT_DF_SCHEMA = StructType([
    StructField("medical_record_number", StringType(), True)\
    ,StructField("date_of_birth", StringType(), True)\
    ,StructField("month_of_birth", StringType(), True)\
    ,StructField("gender", StringType(), True)\
    ,StructField("religion", StringType(), True)\
    ,StructField("race", StringType(), True)\
    ,StructField("patient_ethnic_group", StringType(), True)\
    ,StructField("deceased_indicator", StringType(), True)\
    ,StructField("mother_account_number", StringType(), True)\
    ,StructField("address_zip", StringType(), True)\
    ,StructField("marital_status_code", StringType(), True)\
    ,StructField("language", StringType(), True)\
    ,StructField("citizenship", StringType(), True)\
    ,StructField("age_in_days", IntegerType(), True)\
    ,StructField("context_name", StringType(), True)\
    ,StructField("time_of_day_key", DoubleType(), True)\
    ,StructField("caregiver_group_key", StringType(), True)\
    ,StructField("facility_key", StringType(), True)\
    ,StructField("meta_data_key", StringType(), True)\
    ,StructField("description", StringType(), True)\
    ,StructField("context_diagnosis_code", StringType(), True)\
    ,StructField("encounter_key", StringType(), True)\
    ,StructField("procedure_description", StringType(), True)\
    ,StructField("context_procedure_code", StringType(), True)\
    ,StructField("procedure_control_key", StringType(), True)\
    ,StructField("value", StringType(), True)\
    ,StructField("material_name", StringType(), True)\
    ,StructField("context_material_code", StringType(), True)\
    ,StructField("begin_date_age_in_days", StringType(), True)\
    ,StructField("end_date_age_in_days", StringType(), True)\
    ,StructField("encounter_visit_id", StringType(), True)\
    ,StructField("level1_context_name", StringType(), True)\
    ,StructField("level2_event_name", StringType(), True)\
    ,StructField("level3_action_name", StringType(), True)\
    ,StructField("level4_field_name", StringType(), True)])

def create_spark_df(spark, pandas_df, schema=None):
    """
    Creates a spark dataframe from a pandas dataframe

    Keyword arguments:
    spark -- the spark session
    pandas_df -- the pandas dataframe
    """
    pandas_df = define_column_types_for_patient_events(pandas_df)
    return spark.createDataFrame(pandas_df, schema=schema)


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
    """
    Redefines the type of columns belonging to the patient_events Pandas DataFrame to strings.
    """
    patient_events['date_of_birth'] = patient_events['date_of_birth'].astype(
        str)
    patient_events['religion'] = patient_events['religion'].astype(str)
    patient_events['patient_ethnic_group'] = patient_events['patient_ethnic_group'].astype(
        str)
    patient_events['language'] = patient_events.language.astype(str)
    return patient_events


@timer
def merge_dataframes(left: pd.DataFrame, right: pd.DataFrame, join_columns: list) -> pd.DataFrame:
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
    result = deduplicated_left.merge(right=deduplicated_right,
                                     on=join_columns, how='inner')
    del left
    del right
    return result
