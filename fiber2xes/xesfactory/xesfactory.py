import uuid

from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer
from opyenxes.factory.XFactory import XFactory

from ..translation import Translation
from ..abstraction import Abstraction


def translate_procedure_diagnosis_material_to_event(abstraction_path, abstraction_exact_match, abstraction_delimiter,
                                                    event, verbose, remove_unlisted):
    if not Translation.is_known_event(event):
        return None, None, None, None

    event_name, event_type, event_context, event_code = Translation.translate_to_event(event, verbose)

    abstract_event_name = Abstraction.get_abstract_event_name(abstraction_path, abstraction_exact_match,
                                                              abstraction_delimiter, event_name, remove_unlisted)

    if abstract_event_name is None:
        return None, event_name, event_context, event_code
    elif not verbose:
        return abstract_event_name, event_name, event_context, event_code

    result = event_type

    if event_context is not None and verbose:
        result += (" (" + event_context + " " + event_code + ")")

    if event_name is not None:
        result += (": " + abstract_event_name)

    return result, event_name, event_context, event_code


def create_xes_trace(trace_events, event_filter, abstraction_path, abstraction_exact_match, abstraction_delimiter,
                     verbose, remove_unlisted, remove_duplicates):
    """Collect events that belong to a trace in an opyenxes trace.

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

    trace.get_attributes()["patient:mrn"] = XFactory.create_attribute_literal(
        "patient:mrn", trace_events[0].medical_record_number)
    trace.get_attributes()["patient:date_of_birth"] = XFactory.create_attribute_literal(
        "patient:date_of_birth", trace_events[0].date_of_birth)
    trace.get_attributes()["patient:address_zip"] = XFactory.create_attribute_literal(
        "patient:address_zip", trace_events[0].address_zip)
    trace.get_attributes()["patient:gender"] = XFactory.create_attribute_literal(
        "patient:gender", trace_events[0].gender)
    trace.get_attributes()["patient:language"] = XFactory.create_attribute_literal(
        "patient:language", trace_events[0].language)
    trace.get_attributes()["patient:patient_ethnic_group"] = XFactory.create_attribute_literal(
        "patient:patient_ethnic_group", trace_events[0].patient_ethnic_group)
    trace.get_attributes()["patient:race"] = XFactory.create_attribute_literal(
        "patient:race", trace_events[0].race)
    trace.get_attributes()["patient:religion"] = XFactory.create_attribute_literal(
        "patient:religion", trace_events[0].religion)
    trace.get_attributes()["patient:citizenship"] = XFactory.create_attribute_literal(
        "patient:citizenship", trace_events[0].citizenship)
    trace.get_attributes()["patient:marital_status_code"] = XFactory.create_attribute_literal(
        "patient:marital_status_code", trace_events[0].marital_status_code)

    relevant_events = list()

    # Filter out events that do not match the specified events filter
    for event in trace_events:
        is_relevant = False
        if event_filter is None:
            is_relevant = True
        else:
            is_relevant = event_filter.is_relevant_event(event)
        if not is_relevant:
            continue

        event_descriptor, event_name, event_context, event_code = \
            translate_procedure_diagnosis_material_to_event(
                abstraction_path=abstraction_path,
                abstraction_exact_match=abstraction_exact_match,
                abstraction_delimiter=abstraction_delimiter,
                event=event,
                verbose=verbose,
                remove_unlisted=remove_unlisted
            )
        if event_descriptor is not None:
            event = {
                "timestamp": event.timestamp,
                "name": event_descriptor,
                "description": event_name,
                "context": event_context,
                "code": event_code,
                "caregiver_group_key": event.caregiver_group_key,
                "facility_key": event.facility_key
            }
            relevant_events.append(event)

    if len(relevant_events) == 0:
        return trace

    if remove_duplicates:
        # Remove events with the same name and timestamp
        unique_values = set()
        deduplicated_events = list()
        for event in relevant_events:
            if not (event["timestamp"], event["name"]) in unique_values:
                unique_values.add((event["timestamp"], event["name"]))
                deduplicated_events.append(event)
        relevant_events = deduplicated_events

    for event in relevant_events:
        # Create opyenxes event and append it to the trace
        log_event = XFactory.create_event()

        timestamp_int = event["timestamp"]
        timestamp_attribute = XFactory.create_attribute_timestamp(
            "time:timestamp", timestamp_int)
        log_event.get_attributes()["timestamp"] = timestamp_attribute

        activity_attribute = XFactory.create_attribute_literal(
            "concept:name", event["name"])
        log_event.get_attributes()["Activity"] = activity_attribute

        description_attribute = XFactory.create_attribute_literal(
            "event:description", event["description"])
        log_event.get_attributes()["event:description"] = description_attribute

        context_attribute = XFactory.create_attribute_literal(
            "event:context", event["context"])
        log_event.get_attributes()["event:context"] = context_attribute

        code_attribute = XFactory.create_attribute_literal(
            "event:code", event["code"])
        log_event.get_attributes()["event:code"] = code_attribute

        caregiver_attribute = XFactory.create_attribute_literal(
            "event:caregiver_group", event["caregiver_group_key"]
        )
        log_event.get_attributes()["event:caregiver_group"] = caregiver_attribute

        facility_attribute = XFactory.create_attribute_literal(
            "event:facility", event["facility_key"]
        )
        log_event.get_attributes()["event:facility"] = facility_attribute

        trace.append(log_event)
    return trace


def create_xes_traces_from_traces(traces, abstraction_path, abstraction_exact_match, abstraction_delimiter, verbose,
                                  remove_unlisted, event_filter, remove_duplicates):
    """Create opyenxes traces for every trace.

    Keyword arguments:
    traces -- spark data frame containing all relevant traces
    abstraction_path -- path to the abstraction table stored as a .csv-file
    abstraction_delimiter -- column delimiter used in abstraction table
    abstraction_exact_match -- match only keywords that are identical to the given event name
    verbose -- flag to enable detailed console output
    remove_unlisted -- remove all events that are not included in the abstraction table
    event_filter -- a custom filter to filter events
    remove_duplicates -- flag for remove duplicate events in a trace
    """
    result = traces\
        .map(lambda trace: create_xes_trace(
            trace[1],
            event_filter,
            abstraction_path,
            abstraction_exact_match,
            abstraction_delimiter,
            verbose,
            remove_unlisted,
            remove_duplicates
        ))
    return result.collect()


def save_event_log_to_file(log, file_path):
    """This method serialises a created log to a file.

    Keyword arguments:
    log -- the log generated by the `cohort_to_event_log` method
    file_path -- the file path / name
    """
    with open(file_path, "w") as file:
        XesXmlSerializer().serialize(log, file)
