import uuid
from opyenxes.factory.XFactory import XFactory
from ..translation import Translation
from ..abstraction import get_abstract_event_name


def translate_procedure_diagnosis_material_to_event(event, verbose):

    if not Translation.is_known_event(event):
        return None, None, None, None

    event_name, event_type, event_context, event_code = Translation.translate_to_event(
        event, verbose)

    abstract_event_name = get_abstract_event_name(event_name)

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


def process_partition_events_to_traces(lock, return_dict, process_index, event_range,
                                       verbose, event_filter, patients):
    # iterate over MRN
    # iterate over encounter
    # create trace per encounter
    # translate events to proper types
    # add events of encounter to trace
    if verbose:
        print("[Subprocess " + str(process_index) + "] - Start processing log partition")

    event_counter = 0

    traces = []
    for mrn in event_range:
        trace_id = 0
        patient_data = patients.loc[patients["medical_record_number"] == mrn]
        for trace_key in event_range[mrn]:
            trace = XFactory.create_trace()

            id_attribute = XFactory.create_attribute_id(
                "id", str(uuid.uuid4()))
            trace.get_attributes()["id"] = id_attribute

            if patient_data is not None:
                trace.get_attributes()["patient:date_of_birth"] = XFactory.create_attribute_literal(
                    "patient:date_of_birth", patient_data["date_of_birth"].values[0])
                trace.get_attributes()["patient:address_zip"] = XFactory.create_attribute_literal(
                    "patient:address_zip", patient_data["address_zip"].values[0])
                trace.get_attributes()["patient:gender"] = XFactory.create_attribute_literal(
                    "patient:gender", patient_data["gender"].values[0])
                trace.get_attributes()["patient:language"] = XFactory.create_attribute_literal(
                    "patient:language", patient_data["language"].values[0])
                trace.get_attributes()["patient:patient_ethnic_group"] = XFactory.create_attribute_literal(
                    "patient:patient_ethnic_group", patient_data["patient_ethnic_group"].values[0])
                trace.get_attributes()["patient:race"] = XFactory.create_attribute_literal(
                    "patient:race", patient_data["race"].values[0])
                trace.get_attributes()["patient:religion"] = XFactory.create_attribute_literal(
                    "patient:religion", patient_data["religion"].values[0])
                trace.get_attributes()["patient:citizenship"] = XFactory.create_attribute_literal(
                    "patient:citizenship", patient_data["citizenship"].values[0])
                trace.get_attributes()["patient:marital_status_code"] = XFactory.create_attribute_literal(
                    "patient:marital_status_code", patient_data["marital_status_code"].values[0])
            trace_id = trace_id + 1

            event_counter += len(event_range[mrn][trace_key])

            for event in event_range[mrn][trace_key]:
                is_relevant = False
                if event_filter is None:
                    is_relevant = True
                else:
                    is_relevant = event_filter.is_relevant_event(event)

                if not is_relevant:
                    continue

                event_descriptor, event_name, event_context, event_code = \
                    translate_procedure_diagnosis_material_to_event(event=event, verbose=verbose)
                if event_descriptor is not None:
                    log_event = XFactory.create_event()

                    timestamp_int = event.timestamp
                    timestamp_attribute = XFactory.create_attribute_timestamp(
                        "time:timestamp", timestamp_int)
                    log_event.get_attributes()["timestamp"] = timestamp_attribute

                    activity_attribute = XFactory.create_attribute_literal(
                        "concept:name", event_descriptor)
                    log_event.get_attributes()["Activity"] = activity_attribute

                    description_attribute = XFactory.create_attribute_literal(
                        "event:description", event_name)
                    log_event.get_attributes()["event:description"] = description_attribute

                    context_attribute = XFactory.create_attribute_literal(
                        "event:context", event_context)
                    log_event.get_attributes()["event:context"] = context_attribute

                    code_attribute = XFactory.create_attribute_literal(
                        "event:code", event_code)
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
            if len(trace) > 0:
                traces.append(trace)

    if verbose:
        print("[Subprocess " + str(process_index) + "] - Done processing log partition")
        print("[Subprocess " + str(process_index) + "] - Processed " + str(len(event_range.items())) + " patients, "
              + str(len(traces)) + " traces, " + str(event_counter) + " events")
        print("[Subprocess " + str(process_index) + "] - Write result")
    lock.acquire()
    return_dict[process_index] = traces
    lock.release()
    if verbose:
        print("[Subprocess " + str(process_index) + "] - Done writing result")
