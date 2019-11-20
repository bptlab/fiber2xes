import pandas as pd
import numpy as np
import csv
import datetime
import time
import math
from fiber import Cohort
from fiber.condition import (
    Procedure,
    Patient,
    Diagnosis,
    VitalSign,
    LabValue,
    Encounter,
    Measurement
)
import os
import re
from opyenxes.factory.XFactory import XFactory
from opyenxes.id.XIDFactory import XIDFactory
from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer
from datetime import datetime as dt

from typing import Optional
from fiber.condition.fact.fact import _FactCondition
from fiber.condition.database import _multi_like_clause
from fiber.condition.mixins import ComparisonMixin
from fiber.database.table import (
    d_pers,
    d_uom,
    fact,
    fd_proc,
    fd_diag,
    fd_mat,
)


class ProcedureWithTime(_FactCondition):
    """
    This is an extension of the Procedure Class, to also contain time of day-keys.
    """
    dimensions = {'PROCEDURE'}
    d_table = fd_proc
    code_column = fd_proc.CONTEXT_PROCEDURE_CODE
    category_column = fd_proc.PROCEDURE_TYPE
    description_column = fd_proc.PROCEDURE_DESCRIPTION

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        fact.TIME_OF_DAY_KEY,
        code_column
    ]


class DiagnosisWithTime(_FactCondition):
    """
    This is an extension of the Diagnosis Class, to also contain time of day-keys.
    """
    dimensions = {'DIAGNOSIS'}
    d_table = fd_diag
    code_column = fd_diag.CONTEXT_DIAGNOSIS_CODE
    category_column = fd_diag.DIAGNOSIS_TYPE
    description_column = fd_diag.DESCRIPTION

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code_column,
        fact.TIME_OF_DAY_KEY,
    ]


class MaterialWithTime(_FactCondition):
    """
    This is an extension of the Material Class, to also contain time of day-keys.
    """
    dimensions = {'MATERIAL'}
    d_table = fd_mat
    code_column = fd_mat.CONTEXT_MATERIAL_CODE
    category_column = fd_mat.MATERIAL_TYPE
    description_column = fd_mat.MATERIAL_NAME

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        code_column
    ]


class DrugWithTime(MaterialWithTime):
    """
    This is an extension of the Drug Class, to also contain time of day-keys.
    """

    def __init__(
        self,
        name: Optional[str] = '',
        brand: Optional[str] = '',
        generic: Optional[str] = '',
        *args,
        **kwargs
    ):
        kwargs['category'] = 'Drug'
        super().__init__(*args, **kwargs)
        self._attrs['name'] = name
        self._attrs['brand'] = brand
        self._attrs['generic'] = generic

    @property
    def name(self):
        return self._attrs['name']

    def _create_clause(self):
        clause = super()._create_clause()
        if self.name:
            clause &= (
                _multi_like_clause(fd_mat.MATERIAL_NAME, self.name) |
                _multi_like_clause(fd_mat.GENERIC_NAME, self.name) |
                _multi_like_clause(fd_mat.BRAND1, self.name) |
                _multi_like_clause(fd_mat.BRAND2, self.name)
            )
        if self._attrs['brand']:
            clause &= (
                _multi_like_clause(fd_mat.BRAND1, self._attrs['brand']) |
                _multi_like_clause(fd_mat.BRAND2, self._attrs['brand'])
            )
        if self._attrs['generic']:
            clause &= _multi_like_clause(
                fd_mat.GENERIC_NAME, self._attrs['generic'])

        return clause


# TODO: investigate empty traces
# TODO: add filtering based on condition

PROCEDURE_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "msdw-vocabularies", "vocab-procedure.csv")
DIAGNOSIS_ICD_10_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "msdw-vocabularies", "vocab-icd10.csv")
DIAGNOSIS_ICD_9_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "msdw-vocabularies", "vocab-icd9.csv")

def timestamp_from_birthdate_and_age_and_time(date, age_in_days, time_of_day_key):
    if math.isnan(age_in_days):
        return date
    else:
        timestamp_without_hours_and_minutes = date + \
            datetime.timedelta(days=age_in_days)
        date_without_time = pd.to_datetime(
            timestamp_without_hours_and_minutes, errors='coerce')
        date_with_time = date_without_time + \
            datetime.timedelta(minutes=time_of_day_key)
        return date_with_time


def timestamp_from_birthdate_and_age(date, age_in_days):
    if math.isnan(age_in_days):
        return date
    else:
        return date + datetime.timedelta(days=age_in_days)


def get_encounters_per_patient(patients, encounters):
    patient_mrns = patients.medical_record_number.unique()
    encounters_per_patient = {}
    for mrn in patient_mrns:
        encounters_for_patient = {}
        all_encounters_for_patient = encounters[(
            encounters.medical_record_number == mrn)]
        for index, encounter in all_encounters_for_patient.iterrows():
            if encounter['timestamp'] in encounters_for_patient:
                if encounter['timestamp_end'] > encounters_for_patient[encounter['timestamp']]['end_timestamp']:
                    encounters_for_patient[encounter['timestamp']] = {
                        "begin_timestamp": encounter['timestamp'],
                        "end_timestamp": encounter['timestamp_end'],
                        "mrn": encounter['medical_record_number']}
            else:
                encounters_for_patient[encounter['timestamp']] = {
                    "begin_timestamp": encounter['timestamp'],
                    "end_timestamp": encounter['timestamp_end'],
                    "mrn": encounter['medical_record_number']
                }
        encounters_per_patient[mrn] = encounters_for_patient
    return encounters_per_patient


def get_patient_events(patients, events):
    # join patients and events
    patient_events = pd.merge(
        patients, events, on='medical_record_number', how='outer')

    patient_events['timestamp'] = patient_events.apply(lambda row: timestamp_from_birthdate_and_age_and_time(
        row.date_of_birth, row.age_in_days, row.time_of_day_key), axis=1)

    return patient_events


def get_patient_encounters(patients, encounters):
    # join encounters and encounters
    patient_encounters = pd.merge(
        patients, encounters, on='medical_record_number', how='outer')

    patient_encounters['timestamp'] = patient_encounters.apply(lambda row: timestamp_from_birthdate_and_age(
        row.date_of_birth, row.age_in_days), axis=1)

    patient_encounters['timestamp_end'] = patient_encounters.apply(lambda row: timestamp_from_birthdate_and_age(
        row.timestamp, row.end_date_age_in_days), axis=1)

    pd.to_datetime(patient_encounters.timestamp, errors='coerce')
    return patient_encounters


def get_encounter_event_per_patient(patients, patient_encounter_buckets, events):
    patient_mrns = patients.medical_record_number.unique()
    encounter_events_per_patient = {}
    for mrn in patient_mrns:
        patient_encounters = patient_encounter_buckets[mrn]
        patient_events = events[(
            events.medical_record_number == mrn)]
        patient_events_per_encounter = {}
        for encounter_begin_date in patient_encounters:
            encounter_end_date = patient_encounters[encounter_begin_date]['end_timestamp']
            for index, event in patient_events.iterrows():
                if (event.timestamp == encounter_begin_date and event.timestamp < encounter_end_date) or (event.timestamp > encounter_begin_date and event.timestamp < encounter_end_date) or (event.timestamp > encounter_begin_date and event.timestamp == encounter_end_date) or (event.timestamp == encounter_begin_date and event.timestamp == encounter_end_date):
                    existing_events_per_encounter = patient_events_per_encounter.get(
                        encounter_begin_date)
                    if existing_events_per_encounter is None:
                        existing_events_per_encounter = [event]
                    else:
                        existing_events_per_encounter = existing_events_per_encounter + \
                            [event]
                    patient_events_per_encounter[encounter_begin_date] = existing_events_per_encounter
        encounter_events_per_patient[mrn] = patient_events_per_encounter
    return encounter_events_per_patient


def filter_encounter_events(encounter_events, relevant_diagnosis=None, relevant_procedure=None, relevant_material=None, filter_expression=None):
    # iterate over MRN
    # iterate over encounter
    # iterate over events
    # if no event matches description, drop encounter

    filtered_encounter_events = {}
    for mrn in encounter_events:
        for begin_date in encounter_events[mrn]:
            is_relevant = False
            if relevant_diagnosis is not None:
                if has_diagnosis(relevant_diagnosis, encounter_events[mrn][begin_date]):
                    is_relevant = True
            if relevant_procedure is not None:
                if is_relevant or has_procedure(relevant_diagnosis, encounter_events[mrn][begin_date]):
                    is_relevant = True
            if relevant_material is not None:
                if is_relevant or has_material(relevant_diagnosis, encounter_events[mrn][begin_date]):
                    is_relevant = True
            if filter_expression is not None:
                if is_relevant or filter_expression(encounter_events[mrn][begin_date]):
                    is_relevant = True

            if is_relevant:
                if mrn not in filtered_encounter_events:
                    filtered_encounter_events[mrn] = {}
                if begin_date not in filtered_encounter_events[mrn]:
                    filtered_encounter_events[mrn][begin_date] = {}
                filtered_encounter_events[mrn][begin_date] = encounter_events[mrn][begin_date]

    return filtered_encounter_events


def has_diagnosis(diagnosis, encounter):
    for event in encounter:
        if event.context_diagnosis_code == diagnosis:
            return True
    return False


def has_procedure(procedure, encounter):
    for event in encounter:
        if event.context_procedure_code == procedure:
            return True
    return False


def has_material(material, encounter):
    for event in encounter:
        if event.context_material_code == material:
            return True
    return False


def create_log_from_filtered_encounter_events(filtered_encounter_events):
    # iterate over MRN
    # iterate over encounter
    # create trace per encounter
    # translate events to proper types
    # add events of encounter to trace

    log = XFactory.create_log()
    for mrn in filtered_encounter_events:
        encounter_id = 0
        for encounter in filtered_encounter_events[mrn]:
            trace = XFactory.create_trace()

            id_attribute = XFactory.create_attribute_id(
                "id", str(mrn) + "_" + str(encounter_id))
            trace.get_attributes()["id"] = id_attribute
            encounter_id = encounter_id + 1

            for event in filtered_encounter_events[mrn][encounter]:
                    context_diagnosis_name=event.context_name, 
                    context_material_code=
                event_descriptor = translate_procedure_diagnosis_material_to_event(
                    context_diagnosis_code=event.context_diagnosis_code, 
                    context_diagnosis_name=event.context_name, 
                    context_material_code=event.context_material_code,
                    context_procedure_code=event.context_procedure_code)
                if event_descriptor is not None:
                    log_event = XFactory.create_event()
                    timestamp_int = event.timestamp
                    timestamp_attribute = XFactory.create_attribute_timestamp(
                        "time:timestamp", timestamp_int)
                    activity_attribute = XFactory.create_attribute_literal(
                        "concept:name", event_descriptor)
                    log_event.get_attributes(
                    )["timestamp"] = timestamp_attribute
                    log_event.get_attributes()["Activity"] = activity_attribute
                    trace.append(log_event)
            log.append(trace)
    return log

def vocabulary_lookup(vocabulary_path, search_term, search_column = 0, target_column = 1, delimiter = ","):
    reader = csv.reader(open(vocabulary_path), delimiter=delimiter)
    for row in reader:
        if len(row) > search_column and len(row) > target_column:
            if re.search("^" + search_term + "$", row[search_column], re.IGNORECASE) != None:
                return row[target_column]
    return None

def translate_procedure_diagnosis_material_to_event(context_diagnosis_code, context_diagnosis_name, context_procedure_code, context_material_code):
    """
    When is diagnosis the event? When is procedure the event?

    encounter_type set
    context_diagnosis_code = MSDW_NOT_APPLICABLE | context_diagnosis_code = MSDW_UNKNOWN
    context_procedure_code set
    ->  procedure is event

    encounter_type set
    context_procedure_code = MSDW_NOT_APPLICABLE
    context_diagnosis_code set
    -> diagnosis is event
    """
    
    event_name = None
    event_name_prefix = None

    if context_procedure_code != "MSDW_NOT APPLICABLE" and context_procedure_code != "MSDW_UNKNOWN":
        # Event is procedure
        event_name_prefix = "PROCEDURE"
        event_name = context_procedure_code
        event_name = vocabulary_lookup(
            vocabulary_path = PROCEDURE_VOCAB_PATH, 
            search_term = str(context_procedure_code), 
            search_column = 0, 
            target_column = 1
        )
    elif context_diagnosis_code != "MSDW_NOT APPLICABLE" and context_diagnosis_code != "MSDW_UNKNOWN":
        # Event is diagnosis
        event_name_prefix = "DIAGNOSIS"
        event_name = context_diagnosis_code
        if context_diagnosis_name.str.contains("ICD-10", regex=False).any():
            event_name_prefix = "ICD-10"
            event_name = vocabulary_lookup(
                vocabulary_path = DIAGNOSIS_ICD_10_VOCAB_PATH, 
                search_term = str(context_diagnosis_code), 
                search_column = 0, 
                target_column = 1
            )
        elif context_diagnosis_name.str.contains("ICD-9", regex=False).any():
            event_name_prefix = "ICD-9"
            event_name = vocabulary_lookup(
                vocabulary_path = DIAGNOSIS_ICD_9_VOCAB_PATH, 
                search_term = str(context_diagnosis_code), 
                search_column = 0, 
                target_column = 1
            )
        else:
            print("Could not lookup diagnosis code " + context_diagnosis_code + " for " + context_diagnosis_name)
    elif context_material_code != "MSDW_NOT APPLICABLE" and context_material_code != "MSDW_UNKNOWN":
        return "MATERIAL_" + str(context_material_code)
    else:
        # Event is neither procedure nor event
        return None
    
    if event_name_prefix != None and event_name != None:
        return event_name_prefix + ": " + event_name
    
    return None


def log_from_cohort(cohort, relevant_diagnosis=None, relevant_procedure=None, relevant_material=None, filter_expression=None):
    # get necessary data from cohort
    patients = cohort.get(Patient())
    encounters = cohort.get(Encounter())
    events = cohort.get(DiagnosisWithTime(),
                        ProcedureWithTime(), DrugWithTime())

    """
    Encounter -> Case
    1. Get encounters for a patient
    2. Get procedures, diagnoses that happened for patient in timespan of encounter
    3. If one procedure/diagnosis of encounter matches condition
            -> Encounter (+ procedures/diagnoses) is part of case
    """

    patient_events = get_patient_events(patients, events)

    patient_encounters = get_patient_encounters(patients, encounters)

    patient_encounter_buckets = get_encounters_per_patient(
        patients, patient_encounters)

    encounter_events_per_patient = get_encounter_event_per_patient(
        patients, patient_encounter_buckets, patient_events)

    filtered_encounter_events = filter_encounter_events(
        encounter_events_per_patient,
        relevant_diagnosis=relevant_diagnosis,
        relevant_procedure=relevant_procedure,
        relevant_material=relevant_material,
        filter_expression=filter_expression)

    log = create_log_from_filtered_encounter_events(filtered_encounter_events)

    return log


def save_log_to_file(log):
    with open("event_log.xes", "w") as file:
        XesXmlSerializer().serialize(log, file)
