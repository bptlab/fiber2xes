"""
Provides a helper class for XES trace and log generation
"""
import uuid


from typing import (
    List,
    Optional,
    Tuple
)
from opyenxes.factory.XFactory import XFactory  # type: ignore
from opyenxes.factory.XFactory import XTrace
from opyenxes.factory.XFactory import XEvent

from fiber2xes.filter.Filter import Filter

from .deduplication_helper import (
    identify_duplicate_special_events, apply_time_lifecycle_duplicate_detection)

from ..abstraction.abstraction import get_abstract_event_name
from ..translation import (Translator, is_known_event)


class TraceHelper():
    """
    A class which provides helper functions for creating XES traces out of the extracted events.
    """

    def __init__(self, abstraction_path: str,
                 abstraction_exact_match: bool,
                 abstraction_delimiter: str,
                 verbose: bool,
                 remove_unlisted: bool,
                 include_anamnesis_events: bool,
                 perform_complex_duplicate_detection: bool,
                 duplicate_event_identifier: Optional[str],
                 event_identifier_to_merge: Optional[str],
                 ):
        self.abstraction_path = abstraction_path
        self.abstraction_exact_match = abstraction_exact_match
        self.abstraction_delimiter = abstraction_delimiter
        self.verbose = verbose
        self.remove_unlisted = remove_unlisted
        self.include_anamnesis_events = include_anamnesis_events
        self.perform_complex_duplicate_detection = perform_complex_duplicate_detection
        self.duplicate_event_identifier = duplicate_event_identifier
        self.event_identifier_to_merge = event_identifier_to_merge
        self.translator = Translator()

    def translate_procedure_diagnosis_material_to_event(self, event) -> Tuple[Optional[str],
                                                                              Optional[str],
                                                                              Optional[str],
                                                                              Optional[str]]:
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

        if not is_known_event(event):
            return None, None, None, None

        event_name, event_description, event_type, anamnesis, event_context, event_code = \
            self.translator.translate_to_event(event, self.verbose)

        abstract_event_name = get_abstract_event_name(self.abstraction_path,
                                                      self.abstraction_exact_match,
                                                      self.abstraction_delimiter,
                                                      event_name,
                                                      self.remove_unlisted,
                                                      anamnesis,
                                                      self.include_anamnesis_events)

        if abstract_event_name is None:
            return None, event_description, event_context, event_code
        if not self.verbose:
            return abstract_event_name, event_description, event_context, event_code

        result = event_type

        if event_context is not None and self.verbose:
            result += (" (" + event_context + " " + event_code + ")")

        if event_description is not None:
            result += (": " + abstract_event_name)

        return abstract_event_name, result, event_context, event_code

    def identify_duplicate_medication_diagnosis_events(self,
                                                       trace_events: list) -> Tuple[
                                                           list, dict, dict, dict]:
        """
        Finds the latest event for each medication and diagnosis for each day. This is used to
        chose the right events to discard in the process of removing duplicates while
        keeping the timestamp of the medication/diagnosis as concrete as possible
        """

        seen_end_medications_per_day: dict = {}
        seen_running_medications_per_day: dict = {}
        seen_diagnosis_per_day: dict = {}

        reverse_sorted_trace_events = sorted(
            trace_events, key=lambda e: e.timestamp, reverse=True)

        for event in reverse_sorted_trace_events:

            event_name, event_descriptor, _, _ = \
                self.translate_procedure_diagnosis_material_to_event(event)

            if event_name is not None:

                day = event.timestamp.date()

                if self.perform_complex_duplicate_detection:
                    identify_duplicate_special_events(
                        event, event_name, event_descriptor, day,
                        seen_running_medications_per_day, seen_end_medications_per_day,
                        seen_diagnosis_per_day, trace_events,
                        self.duplicate_event_identifier, self.event_identifier_to_merge)

        return trace_events, seen_end_medications_per_day, \
            seen_running_medications_per_day, seen_diagnosis_per_day

    def apply_event_filters(self, trace_events: list, event_filter: Optional[Filter],
                            seen_running_medications_per_day: dict,
                            seen_end_medications_per_day: dict,
                            seen_diagnosis_per_day: dict) -> Tuple[List[dict], set]:
        """
        Applies the provided filter to each event of the trace. Additionally, it filters out
        all of the earliest prescription/diagnosis/anamnesis events of the same day,
        if duplicates exist.
        Returns the filtered events and a set of visit IDs from which these events stem.
        """

        medication_list: List[str] = list()
        encounter_ids = set()
        relevant_events: List[dict] = list()

        for event in trace_events:
            # Filter out events that do not match the specified events filter

            if event_filter is None:
                is_relevant = True
            else:
                is_relevant = event_filter.is_relevant_event(event)

            # skip handling this event if it is not relevant
            if not is_relevant:
                continue

            # did we see this event earlier?
            is_duplicate = False

            event_name, event_descriptor, event_context, event_code = \
                self.translate_procedure_diagnosis_material_to_event(event)
            if event_name is not None:

                if self.perform_complex_duplicate_detection:
                    lifecycle_state, new_timestamp, is_duplicate = \
                        apply_time_lifecycle_duplicate_detection(
                            event, event_name, event_descriptor, seen_diagnosis_per_day,
                            seen_running_medications_per_day, seen_end_medications_per_day,
                            medication_list, self.duplicate_event_identifier)
                else:
                    lifecycle_state = "unknown"
                    new_timestamp = event.timestamp

                event = create_event_dict(event, event_name, event_descriptor,
                                          event_context, event_code,
                                          lifecycle_state, new_timestamp, is_duplicate)
                relevant_events.append(event)
                encounter_ids.add(event['visit_id'])

        return relevant_events, encounter_ids


def create_traces_based_on_trace_type(trace_type,
                                      relevant_events, trace_information) -> List[XTrace]:
    """
    Either, a single trace containing all passed trace events (i.e., events of a single MRN),
    Creates XTraces based on the passed trace type and the events belonging to a patient:
    or a list of traces with each trace holding all events belonging to one of the patient's
    visits.
    """

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

        for trace_id in encounter_traces:
            xes_traces.append(
                create_trace_object_for_trace_type(trace_information,
                                                   encounter_traces[trace_id],
                                                   trace_type))
    else:
        xes_traces.append(create_trace_object_for_trace_type(
            trace_information, relevant_events, trace_type))

    return xes_traces


def create_event_object(event: dict) -> XEvent:
    """
    Creates an XEvent with its various attributes out of a provided event object,
    which later can be serialised to disk.
    """
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

    is_duplicate_attribute = XFactory.create_attribute_boolean(
        "event:is_duplicate", event["duplicate"]
    )
    log_event.get_attributes()["event:is_duplicate"] = is_duplicate_attribute

    return log_event


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
        "marital": event.marital_status_code,
    }

    return trace_information


def create_trace_object(trace_events: list,
                        trace_type: str, trace_information: dict) -> XTrace:
    """
    Creates an XTrace out of a list of trace events, based on a trace type and trace information.
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
        "patient:marital_status_code", trace_information["marital"])

    return trace


def deduplicate_relevant_events(relevant_events: List[dict]) -> List[dict]:
    """
    Remove events with the same name and timestamp or marked as duplicate.
    It appears that sometimes multiple drugs of the NSAID group are given.
    Therefore the concrete description is used.
    """
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
    return deduplicated_events


def create_event_dict(event, event_name: str, event_descriptor: Optional[str],
                      event_context: Optional[str], event_code: Optional[str],
                      lifecycle_state: str, timestamp, is_duplicate: bool) -> dict:
    """
    Creates a dict containing all necessary event information from a passed event.
    """
    event_dict = {
        "mrn": event.medical_record_number,
        "encounter_id": event.encounter_key,
        "timestamp": timestamp,
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
        "lifecycle": lifecycle_state,
        "duplicate": is_duplicate
    }

    return event_dict


def create_trace_object_for_trace_type(trace_information: dict,
                                       trace_events: list, trace_type: str) -> XTrace:
    """
    Creates an XTrace object for the provided trace information, events, and trace type,
    extracts XEvents out of the provided events, and assigns them to the XTrace object.
    """
    trace = create_trace_object(trace_events=trace_events,
                                trace_information=trace_information,
                                trace_type=trace_type)

    for event in trace_events:
        # Create opyenxes event and append it to the trace
        log_event = create_event_object(event)
        trace.append(log_event)
    return trace
