from datetime import datetime
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class MockEvent:
    def __init__(self,
                 context_procedure_code='MSDW_UNKNOWN',
                 context_diagnosis_code='MSDW_UNKNOWN',
                 context_material_code='MSDW_UNKNOWN',
                 context_name: str = 'MSDW_UNKNOWN',
                 level3_action_name='MSDW_UNKNOWN',
                 description='MSDW_UNKNOWN',
                 procedure_description: str = "MSDW_UNKNOWN",
                 material_name='MSDW_UNKNOWN',
                 timestamp="2020-12-4 08:11:12",
                 date_of_birth="1970-01-01 00:00:00",
                 address_zip="MSDW_UNKNOWN",
                 gender="MSDW_UNKNOWN",
                 language="MSDW_UNKNOWN",
                 patient_ethnic_group="MSDW_UNKNOWN",
                 race="MSDW_UNKNOWN",
                 religion="MSDW_UNKNOWN",
                 marital_status="MSDW_UNKNOWN",
                 medical_record_number="MSDW_UNKNOWN",
                 citizenship="MSDW_UNKNOWN",
                 marital_status_code="MSDW_UNKNOWN",
                 visit_id="MSDW_UNKNOWN",
                 encounter_key="MSDW_UNKNOWN",
                 encounter_visit_id="MSDW_UNKNOWN",
                 caregiver_group_key="MSDW_UNKNOWN",
                 facility_key="MSDW_UNKNOWN",
                 level1_context_name="MSDW_UNKNOWN",
                 level2_event_name="MSDW_UNKNOWN",
                 level4_field_name="MSDW_UNKNOWN",
                 value=None
                 ):
        self.context_procedure_code = context_procedure_code
        self.context_diagnosis_code = context_diagnosis_code
        self.context_material_code = context_material_code
        self.context_name = context_name,
        self.level3_action_name = level3_action_name
        self.description = description
        self.procedure_description = procedure_description
        self.material_name = material_name
        self.timestamp = datetime.strptime(timestamp, DATETIME_FORMAT)
        self.date_of_birth = datetime.strptime(date_of_birth, DATETIME_FORMAT)
        self.address_zip = address_zip
        self.gender = gender
        self.language = language
        self.patient_ethnic_group = patient_ethnic_group
        self.race = race
        self.religion = religion
        self.marital_status = marital_status
        self.medical_record_number = medical_record_number
        self.citizenship = citizenship
        self.marital_status_code = marital_status_code
        self.value = value
        self.visit_id = visit_id
        self.encounter_key = encounter_key
        self.encounter_visit_id = encounter_visit_id
        self.caregiver_group_key = caregiver_group_key
        self.facility_key = facility_key
        self.level1_context_name = level1_context_name
        self.level2_event_name = level2_event_name
        self.level4_field_name = level4_field_name

    def __getitem__(self, item):
        return getattr(self, item)
