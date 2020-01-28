def get_traces_per_patient_by_mrn(patient_events, column_indices):
    return patient_events\
        .rdd\
        .map(lambda row: row + (row[column_indices["medical_record_number"]], ))\
        .toDF(list(patient_events.schema.names) + ["trace_id"])
