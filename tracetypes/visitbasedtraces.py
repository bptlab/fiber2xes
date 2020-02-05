from collections import OrderedDict
import pandas as pd


def get_traces_per_patient_by_visit(patient_event_encounters, column_indices):
    return patient_event_encounters\
        .rdd\
        .map(lambda row: row + (row[column_indices["encounter_visit_id"]], ))\
        .toDF(list(patient_event_encounters.schema.names) + ["trace_id"])
