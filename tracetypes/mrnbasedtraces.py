class MRNBasedTraces(object):
    def get_traces_per_patient(patients, events):
        patient_mrns = patients.medical_record_number.unique()
        traces_per_patient = {}
        for mrn in patient_mrns:
            traces_per_patient[mrn] = {}
            traces_per_patient[mrn][mrn] = []
            patient_events = events[(events.medical_record_number == mrn)]
            for index, event in patient_events.iterrows():
                traces_per_patient[mrn][mrn] = traces_per_patient[mrn][mrn] + [event]
        return traces_per_patient
