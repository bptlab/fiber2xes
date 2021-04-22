"""
Exposes additional functionality
"""

from .list_helper import (create_list, return_object,
                          merge_lists, add_tuple_to_list)

from .helper import timer

from .dataframe_helper import (create_spark_df,
                               handle_duplicate_column_names,
                               define_column_types_for_patient_events,
                               merge_dataframes,
                               EVENT_DF_SCHEMA)

from .event_log_helper import (calculate_timestamp, timestamp_from_birthdate_and_age_and_time,
                               filter_traces, get_traces_per_patient_by_mrn,
                               get_traces_per_patient_by_visit, visit_to_mrn)

__all__ = [
    'create_list',
    'return_object',
    'merge_lists',
    'add_tuple_to_list',
    'timer',
    'create_spark_df',
    'handle_duplicate_column_names',
    'define_column_types_for_patient_events',
    'merge_dataframes',
    'calculate_timestamp',
    'timestamp_from_birthdate_and_age_and_time',
    'filter_traces',
    'get_traces_per_patient_by_mrn',
    'get_traces_per_patient_by_visit',
    'visit_to_mrn',
    'EVENT_DF_SCHEMA'
]
