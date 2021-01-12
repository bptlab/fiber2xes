import datetime
from opyenxes.factory.XFactory import XTrace

from fiber2xes.xesfactory.trace_helper import (
    TraceHelper,
    create_event_dict,
    create_trace_information,
    create_trace_object_for_trace_type,
    create_trace_object, deduplicate_relevant_events)

from mocks.MockEvent import MockEvent

vocabulary_path = "test/test_translation_vocabulary.csv"
abstraction_path = "test/test_abstraction_table.csv"

default_event = MockEvent()

exact_trace_helper_with_unlisted = TraceHelper(
    abstraction_path=abstraction_path,
    abstraction_delimiter=",",
    abstraction_exact_match=True,
    verbose=False,
    remove_unlisted=False,
    include_anamnesis_events=False,
    duplicate_event_identifier=None,
    event_identifier_to_merge=None
)

exact_trace_helper = TraceHelper(
    abstraction_path=abstraction_path,
    abstraction_delimiter=",",
    abstraction_exact_match=True,
    verbose=False,
    remove_unlisted=True,
    include_anamnesis_events=False,
    duplicate_event_identifier=None,
    event_identifier_to_merge=None
)

exact_trace_helper_with_anamnesis = TraceHelper(
    abstraction_path=abstraction_path,
    abstraction_delimiter=",",
    abstraction_exact_match=True,
    verbose=False,
    remove_unlisted=False,
    include_anamnesis_events=True,
    duplicate_event_identifier=None,
    event_identifier_to_merge=None
)


verbose_event = MockEvent(medical_record_number='someMRN',
                          visit_id="someVisit",
                          encounter_visit_id='someVisit',
                          date_of_birth="2020-02-22 00:00:00",
                          address_zip="someZip",
                          gender="someGender",
                          language="someLanguage",
                          patient_ethnic_group="someEthnicGroup",
                          race="someRace",
                          religion="someReligion",
                          citizenship="someCitizenship",
                          marital_status_code="someMaritalStatus")


def test_create_trace_information():
    information = create_trace_information(default_event)
    assert information['mrn'] == "MSDW_UNKNOWN"
    assert information['birth'] == datetime.datetime(1970, 1, 1, 0, 0)
    assert information['zip'] == "MSDW_UNKNOWN"
    assert information['gender'] == "MSDW_UNKNOWN"
    assert information['language'] == "MSDW_UNKNOWN"
    assert information['ethnic'] == "MSDW_UNKNOWN"
    assert information['race'] == "MSDW_UNKNOWN"
    assert information['religion'] == "MSDW_UNKNOWN"
    assert information['citizenship'] == "MSDW_UNKNOWN"
    assert information['marital'] == "MSDW_UNKNOWN"

    verbose_information = create_trace_information(verbose_event)
    assert verbose_information['mrn'] == "someMRN"
    assert verbose_information['birth'] == datetime.datetime(2020, 2, 22, 0, 0)
    assert verbose_information['zip'] == "someZip"
    assert verbose_information['gender'] == "someGender"
    assert verbose_information['language'] == "someLanguage"
    assert verbose_information['ethnic'] == "someEthnicGroup"
    assert verbose_information['race'] == "someRace"
    assert verbose_information['religion'] == "someReligion"
    assert verbose_information['citizenship'] == "someCitizenship"
    assert verbose_information['marital'] == "someMaritalStatus"


def test_create_trace_object():
    trace_information = create_trace_information(verbose_event)

    mrn_trace = create_trace_object(
        trace_events=[verbose_event], trace_type="mrn", trace_information=trace_information)

    assert type(mrn_trace) == XTrace
    assert mrn_trace.get_attributes().get("concept:name").get_value() == 'someMRN'

    visit_trace = create_trace_object(
        trace_events=[verbose_event], trace_type="visit", trace_information=trace_information)
    assert type(visit_trace) == XTrace
    assert visit_trace.get_attributes().get(
        "concept:name").get_value() == 'someVisit'


def test_deduplicate_relevant_events():
    verbose_event_dict = create_event_dict(
        event=verbose_event, event_name="someName",
        event_descriptor=None, event_context=None, event_code=None,
        timestamp=verbose_event.timestamp,
        lifecycle_state="someState")

    default_event_dict = create_event_dict(
        event=default_event, event_name="defaultName",
        event_descriptor=None, event_context=None, event_code=None,
        timestamp=verbose_event.timestamp,
        lifecycle_state="defaultState")
    deduplicated_events = deduplicate_relevant_events(
        [verbose_event_dict, verbose_event_dict]
    )
    assert len(deduplicated_events) == 1

    deduplicated_events_two = deduplicate_relevant_events(
        [verbose_event_dict, default_event_dict, default_event_dict, verbose_event_dict])

    assert len(deduplicated_events_two) == 2


def test_create_event_dict():
    dict_verbose = create_event_dict(
        event=verbose_event, event_name="someName",
        event_descriptor="someDescriptor", event_context="someContext",
        event_code="someCode",
        timestamp=verbose_event.timestamp,
        lifecycle_state="someState")

    assert dict_verbose["mrn"] == verbose_event.medical_record_number
    assert dict_verbose["encounter_id"] == verbose_event.encounter_key
    assert dict_verbose["timestamp"] == verbose_event.timestamp
    assert dict_verbose["visit_id"] == verbose_event.encounter_visit_id
    assert dict_verbose["name"] == "someName"
    assert dict_verbose["description"] == "someDescriptor"
    assert dict_verbose["context"] == "someContext"
    assert dict_verbose["code"] == "someCode"
    assert dict_verbose["caregiver_group_key"] == verbose_event.caregiver_group_key
    assert dict_verbose["facility_key"] == verbose_event.facility_key
    assert dict_verbose["level1"] == verbose_event.level1_context_name
    assert dict_verbose["level2"] == verbose_event.level2_event_name
    assert dict_verbose["level3"] == verbose_event.level3_action_name
    assert dict_verbose["level4"] == verbose_event.level4_field_name
    assert dict_verbose["lifecycle"] == "someState"


def test_create_trace_object_for_trace_type():
    trace_information = create_trace_information(verbose_event)
    verbose_event_dict = create_event_dict(
        event=verbose_event, event_name="someName",
        event_descriptor=None, event_context=None, event_code=None,
        timestamp=verbose_event.timestamp,
        lifecycle_state="someState")
    trace_events = [verbose_event_dict]
    trace = create_trace_object_for_trace_type(
        trace_information, trace_events, "mrn")

    assert type(trace) == XTrace
    assert trace.get_attributes().get("concept:name").get_value() == 'someMRN'

    visit_trace = create_trace_object_for_trace_type(
        trace_information, trace_events, "visit")

    assert type(visit_trace) == XTrace
    assert visit_trace.get_attributes().get(
        "concept:name").get_value() == 'someVisit'
