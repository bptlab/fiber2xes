"""
A module which holds a necessary functions to create a xes log for
an array of events.
"""

import uuid
import datetime

from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer  # type: ignore
from opyenxes.factory.XFactory import XFactory  # type: ignore
from opyenxes.factory.XFactory import XTrace
from opyenxes.factory.XFactory import XLog

from pyspark.sql import Row

from .trace_helper import *

from typing import (
    List,
    Optional,
    Tuple
)

from ..abstraction.abstraction import get_abstract_event_name
from ..translation import Translator

from fiber2xes.filter.Filter import Filter


def create_xes_trace_for_events(trace_events: List,
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

    trace_information = create_trace_information(trace_events[0])

    trace_events = sorted(trace_events, key=lambda e: e.timestamp)

    trace_events, seen_end_medications_per_day, seen_running_medications_per_day, seen_diagnosis_per_day = identify_duplicate_medication_diagnosis_events(
        trace_events, abstraction_path, abstraction_exact_match,
        abstraction_delimiter, verbose,
        remove_unlisted, anamnesis_events
    )

    relevant_events, encounter_ids = apply_event_filters(trace_events, event_filter, seen_running_medications_per_day, seen_end_medications_per_day,
                                                         seen_diagnosis_per_day, abstraction_path, abstraction_exact_match, abstraction_delimiter, verbose, remove_unlisted, anamnesis_events)

    if len(relevant_events) == 0:
        return XFactory.create_trace()

    relevant_events = sorted(relevant_events, key=lambda e: (
        e['timestamp'], e['description']))

    if remove_duplicates:
        relevant_events = deduplicate_relevant_events(relevant_events)

    relevant_events = sorted(relevant_events, key=lambda e: e['timestamp'])

    return create_XES_trace_based_on_trace_type(trace_type, relevant_events, trace_information)


def create_xes_traces_from_traces(traces,
                                  abstraction_path: str,
                                  abstraction_exact_match: bool,
                                  abstraction_delimiter: str,
                                  verbose: bool,
                                  remove_unlisted: bool,
                                  event_filter: Filter,
                                  remove_duplicates: bool,
                                  trace_type: str,
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


def save_event_log_to_file(log: XLog, file_path: str):
    """
    This method serialises a created log to a file.

    Keyword arguments:
    log -- the log generated by the `cohort_to_event_log` method
    file_path -- the file path / name
    """
    with open(file_path, "w") as file:
        XesXmlSerializer().serialize(log, file)
