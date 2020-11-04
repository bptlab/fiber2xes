from datetime import datetime
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class MockEvent:
    def __init__(self,
                 context_procedure_code='MSDW_UNKNOWN',
                 context_diagnosis_code='MSDW_UNKNOWN',
                 context_material_code='MSDW_UNKNOWN',
                 timestamp="2020-12-4 08:11:12"):
        self.context_procedure_code = context_procedure_code
        self.context_diagnosis_code = context_diagnosis_code
        self.context_material_code = context_material_code
        self.timestamp = datetime.strptime(timestamp, DATETIME_FORMAT)
