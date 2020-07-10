import uuid

from opyenxes.data_out.XesXmlSerializer import XesXmlSerializer
from opyenxes.factory.XFactory import XFactory

from ..abstraction import Abstraction
from ..translation import Translation


def translate_procedure_diagnosis_material_to_event(abstraction_path, abstraction_exact_match, abstraction_delimiter,
                                                    event, verbose, remove_unlisted):
    """Derives an activity identifier for an event.

    Keyword arguments:
    abstraction_path -- path to the abstraction table stored as a .csv-file
    abstraction_delimiter -- column delimiter used in abstraction table
    abstraction_exact_match -- match only keywords that are identical to the given event name
    event -- event to be translated
    verbose -- flag to enable detailed console output
    remove_unlisted -- remove all events that are not included in the abstraction table
    """
    if not Translation.is_known_event(event):
        return None, None, None, None

    event_name, event_type, event_context, event_code = Translation.translate_to_event(event, verbose)

    abstract_event_name = Abstraction.get_abstract_event_name(abstraction_path, abstraction_exact_match,
                                                              abstraction_delimiter, event_name, remove_unlisted)

    if abstract_event_name is None:
        return None, event_name, event_context, event_code
    if not verbose:
        return abstract_event_name, event_name, event_context, event_code

    result = event_type

    if event_context is not None and verbose:
        result += (" (" + event_context + " " + event_code + ")")

    if event_name is not None:
        result += (": " + abstract_event_name)

    return result, event_name, event_context, event_code


def create_xes_trace(trace_events, event_filter, abstraction_path, abstraction_exact_match, abstraction_delimiter,
                     verbose, remove_unlisted, remove_duplicates, trace_type):
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

    if trace_type == "mrn":
        trace_id = XFactory.create_attribute_literal("concept:name", trace_events[0].medical_record_number)
    else:
        trace_id = XFactory.create_attribute_literal("concept:name", trace_events[0].encounter_visit_id)

    trace.get_attributes()["concept:name"] = trace_id
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
                remove_unlisted=remove_unlisted
            )
        if event_name is not None:
            enriched_event_name = event_name
            if 'Prescription' in event.level2_event_name or 'Medication' in event.level2_event_name:
                enriched_event_name = event.level4_field_name + ': ' + event_name
            if 'History' in event.level2_event_name or 'Reported' in event.level2_event_name:
                enriched_event_name = 'Anamnesis: ' + event_name

            enriched_event_descriptor = event_descriptor
            if 'Prescription' in event.level2_event_name or 'Medication' in event.level2_event_name:
                enriched_event_descriptor = event.level4_field_name + ': ' + event_descriptor
            if 'History' in event.level2_event_name or 'Reported' in event.level2_event_name:
                enriched_event_descriptor = 'Anamnesis: ' + event_descriptor

            event = {
                "encounter_id": event.encounter_key,
                "timestamp": event.timestamp,
                "visit_id": event.encounter_visit_id,
                "name": enriched_event_name,
                "description": enriched_event_descriptor,
                "context": event_context,
                "code": event_code,
                "caregiver_group_key": event.caregiver_group_key,
                "facility_key": event.facility_key,
                "level1": event.level1_context_name,
                "level2": event.level2_event_name,
                "level3": event.level3_action_name,
                "level4": event.level4_field_name
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

    relevant_events = sorted(relevant_events, key=lambda e: e['timestamp'])

    for event in relevant_events:
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
        log_event.get_attributes()["event:caregiver_group"] = caregiver_attribute

        facility_attribute = XFactory.create_attribute_literal(
            "event:facility", event["facility_key"])
        log_event.get_attributes()["event:facility"] = facility_attribute

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


def create_xes_traces_from_traces(traces, abstraction_path, abstraction_exact_match, abstraction_delimiter, verbose,
                                  remove_unlisted, event_filter, remove_duplicates, trace_type):
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
            remove_duplicates,
            trace_type,
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