import pandas as pd
import numpy as np
import csv
import datetime
from enum import Enum
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
from fiber.condition.database import _case_insensitive_like
from fiber.condition.mixins import ComparisonMixin
from fiber.database.table import (
    d_pers,
    d_enc,
    d_uom,
    fact,
    fd_proc,
    fd_diag,
    fd_mat,
)
import sys


"""
This extends the Encounter Class to also have the Visit key, which allows us to filter based on visits
"""


class EncounterWithVisit(_FactCondition):
    """
    Encounters are parts of the building-blocks of FIBER. In order to define
    Cohorts, Encounters categorize patients as either inpatient or outpatient,
    meaning they stayed in hospital or were treated somewhere else, and define
    categories for this, like 'emergency'.

    The Encounter adds functionality to the FactCondition. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'ENCOUNTER' (and optionally age-constraints on the dates).

    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, ENCOUNTER_TYPE, ENCOUNTER_CLASS, BEGIN_DATE_AGE_IN_DAYS and
    END_DATE_AGE_IN_DAYS in this case respectively.
    """
    dimensions = {'ENCOUNTER'}
    d_table = d_enc
    code_column = d_enc.ENCOUNTER_TYPE
    category_column = d_enc.ENCOUNTER_TYPE
    description_column = d_enc.ENCOUNTER_TYPE

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_enc.ENCOUNTER_TYPE,
        d_enc.ENCOUNTER_CLASS,
        d_enc.BEGIN_DATE_AGE_IN_DAYS,
        d_enc.END_DATE_AGE_IN_DAYS,
        d_enc.ENCOUNTER_VISIT_ID,
        d_enc.ENCOUNTER_KEY
    ]

    def __init__(self, category: Optional[str] = '', **kwargs):
        """
        Args:
            category: the category of the encounter to query for
            kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        if category:
            kwargs['category'] = category
        super().__init__(**kwargs)

    def _create_clause(self):
        """
        Used to create a SQLAlchemy clause based on the Encounter-condition.
        It is used to select the correct encoutners based on the category
        provided at initialization-time.
        """
        clause = super()._create_clause()
        if self._attrs['category']:
            clause &= _case_insensitive_like(
                d_enc.ENCOUNTER_TYPE, self._attrs['category'])
        return clause

DIAGNOSIS_ICD_10_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "msdw-vocabularies", "vocab-icd10.csv")
DIAGNOSIS_ICD_9_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "msdw-vocabularies", "vocab-icd9.csv")
PROCEDURE_CPT_4_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "msdw-vocabularies", "vocab-cpt4.csv")

class EventType(Enum):
    DIAGNOSIS = 0
    PROCEDURE = 1
    MATERIAL = 2
    
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
        description_column,
        code_column,
        fact.ENCOUNTER_KEY,
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
        fact.TIME_OF_DAY_KEY,
        description_column,
        code_column,
        fact.ENCOUNTER_KEY,
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
        fact.ENCOUNTER_KEY,
        description_column,
        code_column,
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


def get_visits_and_encounters_per_patient(patients, encounters):
    patient_mrns = patients.medical_record_number.unique()
    visits_and_encounters_per_patient = {}
    for mrn in patient_mrns:
        visits_for_patient = {}
        all_visits_for_patient = encounters[(encounters.medical_record_number == mrn)].encounter_visit_id.unique()
        for encounter_visit_id in all_visits_for_patient:
            visits_for_patient[encounter_visit_id] = []
            all_encounter_keys = encounters[(encounters.encounter_visit_id == encounter_visit_id)].encounter_key.unique()
            for encounter_key in all_encounter_keys:
                visits_for_patient[encounter_visit_id] = visits_for_patient[encounter_visit_id] + [encounter_key]
        visits_and_encounters_per_patient[mrn] = visits_for_patient
    return visits_and_encounters_per_patient

def get_encounter_keys_per_patient(patient_encounters):
    patient_mrns = patient_encounters.medical_record_number.unique()
    encounters_per_patient = {}
    for mrn in patient_mrns:
        encounters_for_patient = patient_encounters[(patient_encounters.medical_record_number == mrn)].encounter_key.unique()
        encounters_per_patient[mrn] = encounters_for_patient
    return encounters_per_patient

def get_patient_events_per_encounter(patients, patient_encounters, patient_events):
    patient_mrns = patients.medical_record_number.unique()
    events_per_patient = {}
    for mrn in patient_mrns:
        events_per_patient[mrn] = {}
        encounters = patient_encounters[mrn]
        for encounter in encounters:
                events_per_patient[mrn][encounter] = []
                events = patient_events[(patient_events.encounter_key == encounter)]
                for index, event in events.iterrows():
                    events_per_patient[mrn][encounter] = events_per_patient[mrn][encounter] + [event] 
    return events_per_patient

def get_patient_encounters(patients, encounters):
    patient_encounters = pd.merge(patients, encounters, on='medical_record_number', how='inner')
    return patient_encounters

def get_patient_events_per_visit(patients, patient_visits_and_encounters, patient_events):
    patient_mrns = patients.medical_record_number.unique()
    events_per_patient = {}
    for mrn in patient_mrns:
        events_per_patient[mrn] = {}
        patient_visits = patient_visits_and_encounters[mrn]
        for visit in patient_visits:
            events_per_patient[mrn][visit] = []
            encounters = patient_visits[visit]
            for encounter in encounters:
                events = patient_events[(patient_events.encounter_key == encounter)]
                for index, event in events.iterrows():
                    events_per_patient[mrn][visit] = events_per_patient[mrn][visit] + [event]
    return events_per_patient

def get_patient_events(patients, events):
    # join patients and events
    patient_events = pd.merge(
        patients, events, on='medical_record_number', how='inner')

    patient_events['timestamp'] = patient_events.apply(lambda row: timestamp_from_birthdate_and_age_and_time(
        row.date_of_birth, row.age_in_days, row.time_of_day_key), axis=1)

    indexes_to_drop = []
    unique_events = set()
    for index, event in patient_events.iterrows():
        tup = (event["medical_record_number"], event["timestamp"],
            event["context_diagnosis_code"], event["context_procedure_code"])
        if tup not in unique_events:
            unique_events.add(tup)
        else:
            indexes_to_drop.append(index)
    patient_events.drop(patient_events.index[indexes_to_drop], inplace=True)
    return patient_events


def filter_events(events_to_filter, relevant_diagnosis=None, relevant_procedure=None, relevant_material=None, filter_expression=None):
    # iterate over MRN
    # iterate over trace keys
    # iterate over events
    # if no event matches description, drop encounter

    filtered_events = {}
    for mrn in events_to_filter:
        for trace_key in events_to_filter[mrn]:
            is_relevant = False
            if relevant_diagnosis is not None:
                if has_diagnosis(relevant_diagnosis, events_to_filter[mrn][trace_key]):
                    is_relevant = True
            if relevant_procedure is not None:
                if is_relevant or has_procedure(relevant_diagnosis, events_to_filter[mrn][trace_key]):
                    is_relevant = True
            if relevant_material is not None:
                if is_relevant or has_material(relevant_diagnosis, events_to_filter[mrn][trace_key]):
                    is_relevant = True
            if filter_expression is not None:
                if is_relevant or filter_expression(events_to_filter[mrn][trace_key]):
                    is_relevant = True

            if is_relevant:
                if mrn not in filtered_events:
                    filtered_events[mrn] = {}
                if trace_key not in filtered_events[mrn]:
                    filtered_events[mrn][trace_key] = {}
                filtered_events[mrn][trace_key] = events_to_filter[mrn][trace_key] 

    return filtered_events


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


def create_log_from_filtered_events(filtered_events):
    # iterate over MRN
    # iterate over encounter
    # create trace per encounter
    # translate events to proper types
    # add events of encounter to trace

    log = XFactory.create_log()
    for mrn in filtered_events:
        trace_id = 0
        for trace_key in filtered_events[mrn]:
            trace = XFactory.create_trace()

            id_attribute = XFactory.create_attribute_id(
                "id", str(mrn) + "_" + str(trace_id))
            trace.get_attributes()["id"] = id_attribute
            trace_id = trace_id + 1

            for event in filtered_events[mrn][trace_key]:
                event_descriptor = translate_procedure_diagnosis_material_to_event(
                    event=event,
                    verbose=True
                )
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

def get_abstract_event_name(event_name, event_type):
    # TODO: Add abstraction vocabularies to merge similar events
    if (event_type is EventType.DIAGNOSIS):
        # TODO
        return event_name
    elif (event_type is EventType.PROCEDURE):
        # TODO
        return event_name
    elif (event_type is EventType.MATERIAL):
        # TODO
        return event_name
    else:
        return event_name

def translate_procedure_diagnosis_material_to_event(event, verbose=False):
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
    
    context_diagnosis_code=event.context_diagnosis_code 
    context_material_code=event.context_material_code
    context_procedure_code=event.context_procedure_code
    context_name=event.context_name
    
    event_name = None
    event_type = ""
    
    # For verbose output
    event_context = None
    event_code = ""

    # Identify event type
    if context_procedure_code != "MSDW_NOT APPLICABLE" and context_procedure_code != "MSDW_UNKNOWN":
        # Event is procedure
        event_type = "PROCEDURE"
        event_code = context_procedure_code
        translation = None
        
        # Look up CPT-4 standard
        if context_name.str.contains("(EPIC )*CPT-4", regex=True).any():
            event_context = "CPT-4"
            translation = vocabulary_lookup(
                vocabulary_path = PROCEDURE_CPT_4_VOCAB_PATH, 
                search_term = str(context_procedure_code), 
                search_column = 1, 
                target_column = 2
            )
        # Look up ICD standard
        elif context_name.str.contains("ICD-10", regex=False).any():
            event_context = "ICD-10"
            translation = vocabulary_lookup(
                vocabulary_path = DIAGNOSIS_ICD_10_VOCAB_PATH, 
                search_term = str(context_procedure_code), 
                search_column = 0, 
                target_column = 1
            )
        elif context_name.str.contains("ICD-9", regex=False).any():
            event_context = "ICD-9"
            translation = vocabulary_lookup(
                vocabulary_path = DIAGNOSIS_ICD_9_VOCAB_PATH, 
                search_term = str(context_procedure_code), 
                search_column = 0, 
                target_column = 1
            )
        elif context_name.str.contains("SYSTEM", regex=False).any():
            event_context = "SYSTEM"
        elif context_name.str.contains("IMO", regex=False).any():
            event_context = "IMO"
        elif context_name.str.contains("EPIC", regex=False).any():
            event_context = "EPIC"
        elif verbose:
            print("Unknown Procedure Context: " + context_name)
            
        if translation is not None:
            event_name = translation
        else:
            event_name = event.procedure_description
        
        event_name = get_abstract_event_name(event_name, EventType.PROCEDURE)
            
    elif context_diagnosis_code != "MSDW_NOT APPLICABLE" and context_diagnosis_code != "MSDW_UNKNOWN":
        # Event is diagnosis
        event_type = "DIAGNOSIS"
        event_code = context_diagnosis_code
        translation = None
        
        # Look up ICD standard
        if context_name.str.contains("ICD-10", regex=False).any():
            event_context = "ICD-10"
            translation = vocabulary_lookup(
                vocabulary_path = DIAGNOSIS_ICD_10_VOCAB_PATH, 
                search_term = str(context_diagnosis_code), 
                search_column = 0, 
                target_column = 1
            )
        elif context_name.str.contains("ICD-9", regex=False).any():
            event_context = "ICD-9"
            translation = vocabulary_lookup(
                vocabulary_path = DIAGNOSIS_ICD_9_VOCAB_PATH, 
                search_term = str(context_diagnosis_code), 
                search_column = 0, 
                target_column = 1
            )            
        elif context_name.str.contains("SYSTEM", regex=False).any():
            event_context = "SYSTEM"
        elif context_name.str.contains("IMO", regex=False).any():
            event_context = "IMO"
        elif context_name.str.contains("EPIC", regex=False).any():
            event_context = "EPIC"
        elif verbose:
            print("Unknown Diagnosis Context: " + context_name)
        
        if translation is not None:
            event_name = translation
        else:
            event_name = event.description
        
        event_name = get_abstract_event_name(event_name, EventType.DIAGNOSIS)

    elif context_material_code != "MSDW_NOT APPLICABLE" and context_material_code != "MSDW_UNKNOWN":
        # Event is material
        event_type = "MATERIAL"
        event_code = context_material_code
        event_name = event.material_name
        
        if context_name.str.contains("EPIC MEDICATION", regex=False).any():
            event_context = "EPIC MEDICATION"
        elif verbose:
            print("Unknown Material Context: " + context_name)
        
        event_name = get_abstract_event_name(event_name, EventType.MATERIAL)
            
    else:
        # Event is neither procedure, material nor diagnosis
        return None
    
    result = event_type
    
    if event_context is not None and verbose:
        result += (" (" + event_context + " " + event_code + ")")
    
    if event_name is not None:
        result += (": " + event_name)
    
    return result

def log_from_cohort(cohort, trace_type, relevant_diagnosis=None, relevant_procedure=None, relevant_material=None, filter_expression=None):
    # get necessary data from cohort
    patients = cohort.get(Patient())
    encounters = cohort.get(EncounterWithVisit())
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

    if trace_type == "encounter":
        # mrn -> encounter_keys
        patient_encounters = get_patient_encounters(patients, encounters)
        patient_encounter_keys = get_encounter_keys_per_patient(patient_encounters)
        # mrn -> encounter_key -> events
        events_per_patient = get_patient_events_per_encounter(patients, patient_encounter_keys, patient_events)
        
    elif trace_type == "visit":
        patient_encounters = get_patient_encounters(patients, encounters)
        # mrn -> encounter_visit_ids -> encounter_key
        patient_visits_and_encounters = get_visits_and_encounters_per_patient(patients, patient_encounters)
        # mrn -> encounter_visit_ids -> events
        events_per_patient = get_patient_events_per_visit(patients, patient_visits_and_encounters, patient_events)
    elif trace_type == "mrn":
        sys.exit("Building traces based on MRN is not yet implemented :(")
    else:
        sys.exit("No matching trace type given. Try using encounter or visit")

    filtered_events = filter_events(
        events_per_patient,
        relevant_diagnosis=relevant_diagnosis,
        relevant_procedure=relevant_procedure,
        relevant_material=relevant_material,
        filter_expression=filter_expression)

    log = create_log_from_filtered_events(filtered_events)

    return log


def save_log_to_file(log):
    with open("event_log.xes", "w") as file:
        XesXmlSerializer().serialize(log, file)
