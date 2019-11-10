import pandas as pd
import numpy as np
import datetime
import time
import math
from opyenxes.factory.XFactory import XFactory
from opyenxes.id.XIDFactory import XIDFactory
from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer


def timestamp_from_birthdate_and_age(date, age_in_days):
    if math.isnan(age_in_days):
        return date
    else:
        return date + datetime.timedelta(days=age_in_days)


def log_from_cohort(cohort):
    # get necessary data from cohort
    patients = cohort.get(Patient())
    events = cohort.get(Encounter(), Diagnosis(), Procedure())

    # join patients and events
    facts = pd.merge(patients, events, on='medical_record_number', how='outer')

    facts['timestamp'] = facts.apply(lambda row: timestamp_from_birthdate_and_age(
        row.date_of_birth, row.age_in_days), axis=1)

    pd.to_datetime(facts.timestamp, errors='coerce')

    facts.drop('gender', axis=1, inplace=True)
    facts.drop('religion', axis=1, inplace=True)
    facts.drop('race', axis=1, inplace=True)
    facts.drop('patient_ethnic_group', axis=1, inplace=True)
    facts.drop('deceased_indicator', axis=1, inplace=True)
    facts.drop('mother_account_number', axis=1, inplace=True)
    facts.drop('address_zip', axis=1, inplace=True)
    facts.drop('marital_status_code', axis=1, inplace=True)
    facts.drop('begin_date_age_in_days', axis=1, inplace=True)
    facts.drop('end_date_age_in_days', axis=1, inplace=True)

    facts = facts.sort_values(['medical_record_number', 'timestamp'],
                              ascending=[False, True])
    display(facts)

    # create log
    log = XFactory.create_log()

    facts_per_patient = dict(tuple(facts.groupby('medical_record_number')))
    for mrn, patient_facts in facts_per_patient.items():
        trace = XFactory.create_trace()
        for index, patient_fact in patient_facts.iterrows():
            # https://github.com/opyenxes/OpyenXes/blob/master/example/Create_random_log.py
            # https://github.com/opyenxes/OpyenXes

            # https://github.com/maxsumrall/xes

            """ 
                When is diagnosis the event? When is encounter the event? When is procedure the event?

                context_procedure_code = MSDW_NOT_APPLICABLE
                context_diagnosis_code = MSDW_NOT_APPLICABLE
                -> encounter_type is event

                encounter_type set
                context_diagnosis_code = MSDW_NOT_APPLICABLE | context_diagnosis_code = MSDW_UNKNOWN
                context_procedure_code set
                ->  procedure is event

                encounter_type set
                context_procedure_code = MSDW_NOT_APPLICABLE
                context_diagnosis_code set
                -> diagnosis is event 
            """

            # Create timestamp attribute
            patient_date = patient_fact["timestamp"]
            timestamp = datetime.datetime(
                patient_date.year, patient_date.month, patient_date.day, 0, 1).timestamp()

            # Add timestamp to event

            # Add event to trace

        # Append trace to log

    print(log)
    return log  # Todo: make this XES file
