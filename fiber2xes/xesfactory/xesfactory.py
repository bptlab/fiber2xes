"""
A module which holds a necessary functions to create a xes log for
an array of events.
"""
from typing import List
from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer  # type: ignore
from opyenxes.factory.XFactory import XFactory  # type: ignore
from opyenxes.factory.XFactory import XTrace
from opyenxes.factory.XFactory import XLog
from fiber2xes.filter.Filter import Filter

from .trace_helper import TraceHelper
from .trace_helper import (create_trace_information,
                           deduplicate_relevant_events,
                           create_traces_based_on_trace_type)


def create_xes_trace_for_events(trace_events: List,
                                event_filter: Filter,
                                abstraction_path: str,
                                abstraction_exact_match: bool,
                                abstraction_delimiter: str,
                                verbose: bool,
                                remove_unlisted: bool,
                                remove_duplicates: bool,
                                trace_type: str,
                                include_anamnesis_events: bool,
                                perform_complex_duplicate_detection: bool,
                                duplicate_event_identifier: str,
                                event_identifier_to_merge: str) -> List[XTrace]:
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
    include_anamnesis_events -- should anamnesis events be included in the xes log
    """

    helper = TraceHelper(abstraction_path, abstraction_exact_match,
                         abstraction_delimiter, verbose, remove_unlisted,
                         include_anamnesis_events, perform_complex_duplicate_detection,
                         duplicate_event_identifier,
                         event_identifier_to_merge)

    trace_information = create_trace_information(trace_events[0])

    trace_events = sorted(trace_events, key=lambda e: e.timestamp)

    trace_events, \
        seen_end_medications_per_day, \
        seen_running_medications_per_day, \
        seen_diagnosis_per_day = helper.identify_duplicate_medication_diagnosis_events(
            trace_events
        )

    relevant_events, _ = helper.apply_event_filters(trace_events, event_filter,
                                                    seen_running_medications_per_day,
                                                    seen_end_medications_per_day,
                                                    seen_diagnosis_per_day)
    if len(relevant_events) == 0:
        return XFactory.create_trace()

    relevant_events = sorted(relevant_events, key=lambda e: (
        e['timestamp'], e['description']))

    if remove_duplicates:
        relevant_events = deduplicate_relevant_events(relevant_events)

    relevant_events = sorted(relevant_events, key=lambda e: e['timestamp'])

    return create_traces_based_on_trace_type(trace_type, relevant_events, trace_information)


def create_xes_traces_from_traces(traces,
                                  abstraction_path: str,
                                  abstraction_exact_match: bool,
                                  abstraction_delimiter: str,
                                  verbose: bool,
                                  remove_unlisted: bool,
                                  event_filter: Filter,
                                  remove_duplicates: bool,
                                  trace_type: str,
                                  include_anamnesis_events: bool,
                                  perform_complex_duplicate_detection: bool,
                                  duplicate_event_identifier: str,
                                  event_identifier_to_merge: str) -> List[List[XTrace]]:
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
    include_anamnesis_events -- should anamnesis events be included in the xes log
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
            include_anamnesis_events,
            perform_complex_duplicate_detection,
            duplicate_event_identifier,
            event_identifier_to_merge
        ))
    return result.collect()


def save_event_log_to_file(log: XLog, file_path: str):
    """
    This method serialises a created log to a file.

    Keyword arguments:
    log -- the log generated by the `cohort_to_event_log` method
    file_path -- the file path / name
    """
    with open(file_path, "w") as file:
        XesXmlSerializer().serialize(log, file)


def save_event_log_to_temporary_file(log: XLog, file):
    """
    This method serializes the created log to a temporary in-memory file for testing only.
    """
    with open(file.name, "w") as opened_temp_file:
        XesXmlSerializer().serialize(log, opened_temp_file)
        opened_temp_file.close()
