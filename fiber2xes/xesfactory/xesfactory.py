"""
A module which holds a necessary functions to create a xes log for
an array of events.
"""

import uuid
import datetime

from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer  # type: ignore
from opyenxes.factory.XFactory import XFactory  # type: ignore
from opyenxes.factory.XFactory import XTrace

from pyspark.sql import Row

from typing import (
    List,
    Optional,
    Tuple
)

from ..abstraction.abstraction import get_abstract_event_name
from ..translation import Translator

from fiber2xes.filter.Filter import Filter


def translate_procedure_diagnosis_material_to_event(abstraction_path: str,
                                                    abstraction_exact_match: bool,
                                                    abstraction_delimiter: str,
                                                    event,
                                                    verbose: bool,
                                                    remove_unlisted: bool,
                                                    anamnesis_events: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Derives an activity identifier for an event.

    Keyword arguments:
    abstraction_path -- path to the abstraction table stored as a .csv-file
    abstraction_delimiter -- column delimiter used in abstraction table
    abstraction_exact_match -- match only keywords that are identical to the given event name
    event -- event to be translated
    verbose -- flag to enable detailed console output
    remove_unlisted -- remove all events that are not included in the abstraction table
    """

    translator = Translator()  # todo: make this a singleton!

    if not translator.is_known_event(event):
        return None, None, None, None

    event_name, event_description, event_type, anamnesis, event_context, event_code = \
        translator.translate_to_event(event, verbose)

    abstract_event_name = get_abstract_event_name(abstraction_path,
                                                  abstraction_exact_match,
                                                  abstraction_delimiter,
                                                  event_name,
                                                  remove_unlisted,
                                                  anamnesis,
                                                  anamnesis_events)

    if abstract_event_name is None:
        return None, event_description, event_context, event_code
    if not verbose:
        return abstract_event_name, event_description, event_context, event_code

    result = event_type

    if event_context is not None and verbose:
        result += (" (" + event_context + " " + event_code + ")")

    if event_description is not None:
        result += (": " + abstract_event_name)

    return abstract_event_name, result, event_context, event_code


def create_trace_information(event) -> dict:
    """
    Creates an object with the information of the trace of the event

    Keyword arguments:
    event - the event which is used to determine the trace information
    """

    trace_information = {
        "mrn": event.medical_record_number,
        "birth": event.date_of_birth,
        "zip": event.address_zip,
        "gender": event.gender,
        "language": event.language,
        "ethnic": event.patient_ethnic_group,
        "race": event.race,
        "religion": event.religion,
        "citizenship": event.citizenship,
        "martial": event.marital_status_code,
    }

    return trace_information


def create_xes_trace_for_events(trace_events,
                                event_filter: Filter,
                                abstraction_path: str,
                                abstraction_exact_match: bool,
                                abstraction_delimiter: str,
                                verbose: bool,
                                remove_unlisted: bool,
                                remove_duplicates: bool,
                                trace_type: str,
                                anamnesis_events: str) -> List[XTrace]:
    """
    Translating the events into event objects, remove duplicated events and
    add lifecycle informations.

    Keyword arguments:
    trace_events -- list of events belonging to a trace
    abstraction_path -- path to the abstraction table stored as a .csv-file
    abstraction_delimiter -- column delimiter used in abstraction table
    abstraction_exact_match -- match only keywords that are identical to the given event name
    verbose -- flag to enable detailed console output
    remove_unlisted -- remove all events that are not included in the abstraction table
    event_filter -- a custom filter to filter events
    remove_duplicates -- flag for remove duplicate events in a trace
    anamnesis_events -- which anamnesis events should be included in the xes log
    """

    relevant_events = list()

    encounter_ids = set()

    trace_information = create_trace_information(trace_events[0])

    trace_events = sorted(trace_events, key=lambda e: e.timestamp)
    reverse_sorted_trace_events = sorted(
        trace_events, key=lambda e: e.timestamp, reverse=True)

    # Finding the latest event for each medication and diagnosis for each day. This is used to
    # chose the right events to discard in the process of removing duplicates while
    # keeping the timestamp of the medication/diagnosis as concrete as possible

    seen_end_medications_per_day: dict = {}
    seen_running_medications_per_day: dict = {}
    seen_diagnosis_per_day: dict = {}

    for event in reverse_sorted_trace_events:

        event_name, event_descriptor, event_context, event_code = \
            translate_procedure_diagnosis_material_to_event(
                abstraction_path=abstraction_path,
                abstraction_exact_match=abstraction_exact_match,
                abstraction_delimiter=abstraction_delimiter,
                event=event,
                verbose=verbose,
                remove_unlisted=remove_unlisted,
                anamnesis_events=anamnesis_events
            )

        if event_name is not None:

            day = event.timestamp.date()

            if ('Prescription' in event.level2_event_name or
                    'Medication' in event.level2_event_name) and \
                    'Anamnesis' not in event_name:

                if day not in seen_running_medications_per_day.keys():
                    seen_running_medications_per_day[day] = {}
                    seen_end_medications_per_day[day] = {}
                if event_descriptor not in seen_end_medications_per_day[day].keys() and \
                        event.level4_field_name == 'End Date':
                    seen_end_medications_per_day[day][event_descriptor] = event
                elif event_descriptor not in seen_running_medications_per_day[day].keys():
                    seen_running_medications_per_day[day][event_descriptor] = event

            # Right now this part of the code is very specific for the use case of low back pain.
            # This has to be adapted to keep fiber2xes open for every possible diagnosis
            elif 'BACK PAIN' in event_name:
                if day not in seen_diagnosis_per_day.keys():
                    seen_diagnosis_per_day[day] = event
                elif event_name == 'CHRONIC LOW BACK PAIN':
                    new_merged_event_dict = event.asDict()
                    new_merged_event_dict['timestamp'] = seen_diagnosis_per_day[day].timestamp
                    new_merged_event = Row(**new_merged_event_dict)
                    seen_diagnosis_per_day[day] = new_merged_event
                    trace_events.append(new_merged_event)

    medication_list = []

    for event in trace_events:
        # Filter out events that do not match the specified events filter

        if event_filter is None:
            is_relevant = True
        else:
            is_relevant = event_filter.is_relevant_event(event)
        if not is_relevant:
            continue

        event_name, event_descriptor, event_context, event_code = \
            translate_procedure_diagnosis_material_to_event(
                abstraction_path=abstraction_path,
                abstraction_exact_match=abstraction_exact_match,
                abstraction_delimiter=abstraction_delimiter,
                event=event,
                verbose=verbose,
                remove_unlisted=remove_unlisted,
                anamnesis_events=anamnesis_events
            )
        if event_name is not None:
            level2 = event.level2_event_name
            level4 = event.level4_field_name
            day = event.timestamp.date()
            lifecycle_state = "complete"

            # if medication related and latest seen event for this medication
            # at this day, set lifecycle:transition corresponding to metadata
            # else mark as duplicate

            if ('Prescription' in level2 or 'Medication' in level2) and \
                    'Anamnesis' not in event_name:

                if event_descriptor in seen_running_medications_per_day[day].keys():
                    if event == seen_running_medications_per_day[day][event_descriptor]:
                        if event_descriptor not in medication_list:
                            medication_list.append(event_descriptor)
                            lifecycle_state = "start"
                        else:
                            medication_list.append(event_descriptor)
                            lifecycle_state = "resume"
                    else:
                        event_name = 'DUPLICATE' + event_name

                elif event_descriptor in seen_end_medications_per_day[day].keys():
                    if event == seen_end_medications_per_day[day][event_descriptor]:
                        if event_descriptor in medication_list:
                            medication_list.remove(event_descriptor)
                        lifecycle_state = "complete"
                    else:
                        event_name = 'DUPLICATE' + event_name

            if 'BACK PAIN' in event_name:
                if event != seen_diagnosis_per_day[day]:
                    event_name = 'DUPLICATE' + event_name

            new_timestamp = event.timestamp
            if 'Start Date' in level4:
                new_timestamp += datetime.timedelta(seconds=1)

            event = {
                "mrn": event.medical_record_number,
                "encounter_id": event.encounter_key,
                "timestamp": new_timestamp,
                "visit_id": event.encounter_visit_id,
                "name": event_name,
                "description": event_descriptor,
                "context": event_context,
                "code": event_code,
                "caregiver_group_key": event.caregiver_group_key,
                "facility_key": event.facility_key,
                "level1": event.level1_context_name,
                "level2": event.level2_event_name,
                "level3": event.level3_action_name,
                "level4": event.level4_field_name,
                "lifecycle": lifecycle_state
            }
            relevant_events.append(event)
            encounter_ids.add(event['visit_id'])

    if len(relevant_events) == 0:
        return XFactory.create_trace()

    relevant_events = sorted(relevant_events, key=lambda e: (
        e['timestamp'], e['description']))

    if remove_duplicates:
        # Remove events with the same name and timestamp or marked as duplicate.
        # It appears that sometimes multiple drugs of the NSAID group are given.
        # Therefore the concrete description is used.
        unique_values = set()
        deduplicated_events = list()
        for event in relevant_events:
            compare_value = event['name']
            if 'NSAID' in compare_value:
                compare_value += event['description']
            if not (event["timestamp"], compare_value) in unique_values and \
                    'DUPLICATE' not in compare_value:
                unique_values.add((event["timestamp"], compare_value))
                deduplicated_events.append(event)
        relevant_events = deduplicated_events

    relevant_events = sorted(relevant_events, key=lambda e: e['timestamp'])

    xes_traces: List[XTrace] = []

    # if visit, there are multiple visits in this trace events
    # -> find those visits and assign events to the corresponding visits
    # -> start xes trace creation for each visit
    if trace_type == 'visit':
        encounter_traces: dict = {}
        for event in relevant_events:
            if event['visit_id'] not in encounter_traces.keys():
                encounter_traces[event['visit_id']] = []
            encounter_traces[event['visit_id']].append(event)

        for trace_id in encounter_traces.keys():
            xes_traces.append(create_xes_trace(trace_information,
                                               encounter_traces[trace_id],
                                               trace_type))
    else:
        xes_traces.append(create_xes_trace(
            trace_information, relevant_events, trace_type))

    return xes_traces


def create_xes_trace(trace_information, trace_events, trace_type) -> XTrace:
    """
    Collect events that belong to a trace in an opyenxes trace.

    Keyword arguments:
    trace_events -- list of events belonging to a trace
    abstraction_path -- path to the abstraction table stored as a .csv-file
    abstraction_delimiter -- column delimiter used in abstraction table
    abstraction_exact_match -- match only keywords that are identical to the given event name
    verbose -- flag to enable detailed console output
    remove_unlisted -- remove all events that are not included in the abstraction table
    event_filter -- a custom filter to filter events
    remove_duplicates -- flag for remove duplicate events in a trace
    """

    trace = XFactory.create_trace()

    if len(trace_events) == 0:
        return trace

    id_attribute = XFactory.create_attribute_id(
        "id", str(uuid.uuid4()))
    trace.get_attributes()["id"] = id_attribute

    if trace_type == "mrn":
        trace_id = XFactory.create_attribute_literal(
            "concept:name", trace_information["mrn"])
    else:
        trace_id = XFactory.create_attribute_literal(
            "concept:name", trace_events[0]['visit_id'])

    trace.get_attributes()["concept:name"] = trace_id
    trace.get_attributes()["patient:mrn"] = XFactory.create_attribute_literal(
        "patient:mrn", trace_information["mrn"])
    trace.get_attributes()["patient:date_of_birth"] = XFactory.create_attribute_literal(
        "patient:date_of_birth", trace_information["birth"])
    trace.get_attributes()["patient:address_zip"] = XFactory.create_attribute_literal(
        "patient:address_zip", trace_information["zip"])
    trace.get_attributes()["patient:gender"] = XFactory.create_attribute_literal(
        "patient:gender", trace_information["gender"])
    trace.get_attributes()["patient:language"] = XFactory.create_attribute_literal(
        "patient:language", trace_information["language"])
    trace.get_attributes()["patient:patient_ethnic_group"] = XFactory.create_attribute_literal(
        "patient:patient_ethnic_group", trace_information["ethnic"])
    trace.get_attributes()["patient:race"] = XFactory.create_attribute_literal(
        "patient:race", trace_information["race"])
    trace.get_attributes()["patient:religion"] = XFactory.create_attribute_literal(
        "patient:religion", trace_information["religion"])
    trace.get_attributes()["patient:citizenship"] = XFactory.create_attribute_literal(
        "patient:citizenship", trace_information["citizenship"])
    trace.get_attributes()["patient:marital_status_code"] = XFactory.create_attribute_literal(
        "patient:marital_status_code", trace_information["martial"])

    for event in trace_events:
        # Create opyenxes event and append it to the trace
        log_event = XFactory.create_event()

        timestamp_int = event["timestamp"]
        timestamp_attribute = XFactory.create_attribute_timestamp(
            "time:timestamp", timestamp_int)
        log_event.get_attributes()["timestamp"] = timestamp_attribute

        activity_attribute = XFactory.create_attribute_literal(
            "concept:name", event['name'])
        log_event.get_attributes()["Activity"] = activity_attribute

        visit_id_attribute = XFactory.create_attribute_literal(
            "event:visit_id", event["visit_id"])
        log_event.get_attributes()["event:visit_id"] = visit_id_attribute

        encounter_attribute = XFactory.create_attribute_literal(
            "event:encounter_id", event["encounter_id"])
        log_event.get_attributes()["event:encounter_id"] = encounter_attribute

        description_attribute = XFactory.create_attribute_literal(
            "event:description", event['description'])
        log_event.get_attributes()["event:description"] = description_attribute

        context_attribute = XFactory.create_attribute_literal(
            "event:context", event["context"])
        log_event.get_attributes()["event:context"] = context_attribute

        code_attribute = XFactory.create_attribute_literal(
            "event:code", event["code"])
        log_event.get_attributes()["event:code"] = code_attribute

        caregiver_attribute = XFactory.create_attribute_literal(
            "event:caregiver_group", event["caregiver_group_key"])
        log_event.get_attributes(
        )["event:caregiver_group"] = caregiver_attribute

        facility_attribute = XFactory.create_attribute_literal(
            "event:facility", event["facility_key"])
        log_event.get_attributes()["event:facility"] = facility_attribute

        lifecycle_attribute = XFactory.create_attribute_literal(
            "lifecycle:transition", event["lifecycle"])
        log_event.get_attributes(
        )["lifecycle:transition"] = lifecycle_attribute

        level1_attribute = XFactory.create_attribute_literal(
            "event:level1", event["level1"])
        log_event.get_attributes()["event:level1"] = level1_attribute

        level2_attribute = XFactory.create_attribute_literal(
            "event:level2", event["level2"])
        log_event.get_attributes()["event:level2"] = level2_attribute

        level3_attribute = XFactory.create_attribute_literal(
            "event:level3", event["level3"])
        log_event.get_attributes()["event:level3"] = level3_attribute

        level4_attribute = XFactory.create_attribute_literal(
            "event:level4", event["level4"])
        log_event.get_attributes()["event:level4"] = level4_attribute

        trace.append(log_event)
    return trace


def create_xes_traces_from_traces(traces,
                                  abstraction_path,
                                  abstraction_exact_match,
                                  abstraction_delimiter,
                                  verbose,
                                  remove_unlisted,
                                  event_filter: Filter,
                                  remove_duplicates,
                                  trace_type,
                                  anamnesis_events) -> List[XTrace]:
    """
    Create opyenxes traces for every trace.

    Keyword arguments:
    traces -- spark data frame containing all relevant traces
    abstraction_path -- path to the abstraction table stored as a .csv-file
    abstraction_delimiter -- column delimiter used in abstraction table
    abstraction_exact_match -- match only keywords that are identical to the given event name
    verbose -- flag to enable detailed console output
    remove_unlisted -- remove all events that are not included in the abstraction table
    event_filter -- a custom filter to filter events
    remove_duplicates -- flag for remove duplicate events in a trace
    anamnesis_events -- which anamnesis events should be included in the xes log
    """
    result = traces\
        .map(lambda trace: create_xes_trace_for_events(
            trace[1],
            event_filter,
            abstraction_path,
            abstraction_exact_match,
            abstraction_delimiter,
            verbose,
            remove_unlisted,
            remove_duplicates,
            trace_type,
            anamnesis_events,
        ))
    return result.collect()


def save_event_log_to_file(log, file_path):
    """
    This method serialises a created log to a file.

    Keyword arguments:
    log -- the log generated by the `cohort_to_event_log` method
    file_path -- the file path / name
    """
    with open(file_path, "w") as file:
        XesXmlSerializer().serialize(log, file)
