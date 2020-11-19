from datetime import datetime
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class MockEvent:
    def __init__(self,
                 context_procedure_code='MSDW_UNKNOWN',
                 context_diagnosis_code='MSDW_UNKNOWN',
                 context_material_code='MSDW_UNKNOWN',
                 context_name='MSDW_UNKNOWN',
                 context_name_1='MSDW_UNKNOWN',
                 context_name_2='MSDW_UNKNOWN',
                 level3_action_name='MSDW_UNKNOWN',
                 procedure_description: str = "MSDW_UNKNOWN",
                 timestamp="2020-12-4 08:11:12",
                 date_of_birth="1970-01-01 00:00:00",
                 address_zip="MSDW_UNKNOWN",
                 gender="MSDW_UNKNOWN",
                 language="MSDW_UNKNOWN",
                 patient_ethnic_group="MSDW_UNKNOWN",
                 race="MSDW_UNKNOWN",
                 religion="MSDW_UNKNOWN",
                 marital_status="MSDW_UNKNOWN",
                 ):
        self.context_procedure_code = context_procedure_code
        self.context_diagnosis_code = context_diagnosis_code
        self.context_material_code = context_material_code
        self.context_name = context_name,
        self.context_name_1 = context_name_1
        self.context_name_2 = context_name_2
        self.level3_action_name = level3_action_name
        self.procedure_description = procedure_description
        self.timestamp = datetime.strptime(timestamp, DATETIME_FORMAT)
        self.date_of_birth = datetime.strptime(date_of_birth, DATETIME_FORMAT)
        self.address_zip = address_zip
        self.gender = gender
        self.language = language
        self.patient_ethnic_group = patient_ethnic_group
        self.race = race
        self.religion = religion
        self.marital_status = marital_status
