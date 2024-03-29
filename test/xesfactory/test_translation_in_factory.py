import copy

from fiber2xes.xesfactory.trace_helper import TraceHelper

from mocks.MockEvent import MockEvent

from fiber2xes.filter.operator import (
    And,
    Or,
    Not
)

from fiber2xes.filter.condition import (
    Diagnosis,
    Generic,
    Material,
    Procedure,
    Time
)

ABSTRACTION_PATH = "test/test_abstraction_table.csv"

unknown_event = MockEvent()
event = MockEvent(medical_record_number='someMRN',
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

procedure_event = MockEvent(medical_record_number='someMRN',
                            context_procedure_code="abstractATermA",
                            procedure_description="abstractATermA",
                            context_name="someContext",
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

procedure_event_anamnesis = MockEvent(medical_record_number='someMRN',
                                      context_procedure_code="abstractATermA",
                                      procedure_description="abstractATermA",
                                      context_name="someContext",
                                      level3_action_name="Reported some Anamnesis",
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


diagnosis_event = MockEvent(medical_record_number='someMRN',
                            context_diagnosis_code="abstractATermA",
                            description="abstractATermA",
                            context_name="someContext",
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


diagnosis_event_anamnesis = MockEvent(medical_record_number='someMRN',
                                      context_diagnosis_code="abstractATermA",
                                      description="abstractATermA",
                                      context_name="someContext",
                                      level3_action_name="Reported some Diagnosis",
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

material_event = MockEvent(medical_record_number='someMRN',
                           context_material_code="abstractATermA",
                           material_name="abstractATermA",
                           context_name="someContext",
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

material_event_anaemnis = MockEvent(medical_record_number='someMRN',
                                    context_material_code="abstractATermA",
                                    material_name="abstractATermA",
                                    level3_action_name="Reported some Material",
                                    context_name="someContext",
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


def test_unknown_event_translation():
    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        unknown_event)
    assert abstract_event_name is None
    assert result is None
    assert event_context is None
    assert event_code is None

    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        event)

    assert abstract_event_name is None
    assert result is None
    assert event_context is None
    assert event_code is None


def test_event_translation():
    # Procedure Events
    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        procedure_event)

    assert abstract_event_name == "AbstractA"
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=True,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    procedure_event_unlisted = copy.copy(procedure_event)
    procedure_event_unlisted.context_procedure_code = "unlisted"
    procedure_event_unlisted.procedure_description = "unlisted"
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        procedure_event_unlisted)

    assert abstract_event_name is None
    assert result == "unlisted"
    assert event_context == "UNKNOWN"
    assert event_code == "unlisted"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        procedure_event_anamnesis)

    assert abstract_event_name is None
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=True,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        procedure_event_anamnesis)

    assert abstract_event_name == "Anamnesis: AbstractA"
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"

    # diagnosis events
    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        diagnosis_event)

    assert abstract_event_name == "AbstractA"
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=True,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    diagnosis_event_unlisted = copy.copy(diagnosis_event)
    diagnosis_event_unlisted.context_diagnosis_code = "unlisted"
    diagnosis_event_unlisted.description = "unlisted"
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        diagnosis_event_unlisted)

    assert abstract_event_name is None
    assert result == "unlisted"
    assert event_context == "UNKNOWN"
    assert event_code == "unlisted"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        diagnosis_event_anamnesis)

    assert abstract_event_name is None
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=True,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        diagnosis_event_anamnesis)

    assert abstract_event_name == "Anamnesis: AbstractA"
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"

    # Material Events
    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        material_event)

    assert abstract_event_name == "AbstractA"
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=True,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    material_event_unlisted = copy.copy(material_event)
    material_event_unlisted.context_material_code = "unlisted"
    material_event_unlisted.material_name = "unlisted"
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        material_event_unlisted)

    assert abstract_event_name is None
    assert result == "unlisted"
    assert event_context == "UNKNOWN"
    assert event_code == "unlisted"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        material_event_anaemnis)

    assert abstract_event_name is None
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"

    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=True,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    abstract_event_name, result, event_context, event_code = trace_helper.translate_procedure_diagnosis_material_to_event(
        material_event_anaemnis)

    assert abstract_event_name == "Anamnesis: AbstractA"
    assert result == "abstractATermA"
    assert event_context == "UNKNOWN"
    assert event_code == "abstractATermA"


def test_duplicate_medication_diagnosis_detection():
    # identify_duplicate_medication_diagnosis_events
    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=True,
                               duplicate_event_identifier="BACK PAIN",
                               event_identifier_to_merge=None
                               )

    material_prescription = copy.copy(material_event)
    material_prescription.level2_event_name = "Medication someMedication"
    back_pain_event = copy.copy(diagnosis_event)
    back_pain_event.context_diagnosis_code = "BACK PAIN"
    back_pain_event.description = "BACK PAIN"
    trace_events, end_meds, running_meds, diagnoses = trace_helper.identify_duplicate_medication_diagnosis_events(
        [material_prescription, material_prescription,
            back_pain_event, back_pain_event]
    )
    assert trace_events == [material_prescription, material_prescription,
                            back_pain_event, back_pain_event]
    assert running_meds == {
        material_prescription.timestamp.date(): {
            'abstractATermA': material_prescription
        }
    }
    assert diagnoses == {
        back_pain_event.timestamp.date():
            back_pain_event

    }
    assert end_meds == {material_prescription.timestamp.date(): {}}


def test_apply_event_filters():
    # apply_event_filters
    trace_helper = TraceHelper(abstraction_path=ABSTRACTION_PATH,
                               abstraction_exact_match=True,
                               abstraction_delimiter=",",
                               verbose=False,
                               remove_unlisted=False,
                               include_anamnesis_events=False,
                               perform_complex_duplicate_detection=False,
                               duplicate_event_identifier=None,
                               event_identifier_to_merge=None
                               )
    material_prescription = copy.copy(material_event)
    material_prescription.level2_event_name = "Medication someMedication"
    back_pain_event = copy.copy(diagnosis_event)
    back_pain_event.context_diagnosis_code = "BACK PAIN"
    back_pain_event.description = "BACK PAIN"
    trace_events = [material_prescription, material_prescription,
                    back_pain_event, back_pain_event]
    trace_events, end_meds, running_meds, diagnoses = trace_helper.identify_duplicate_medication_diagnosis_events(
        trace_events
    )

    events, visits = trace_helper.apply_event_filters(
        trace_events, None, running_meds, end_meds, diagnoses)
    assert visits == {'someVisit'}
    assert events is not None
    assert len(events) == 4

    diagnosis_filter = Diagnosis("BACK PAIN")
    medication_filter = Material("Not Present Material")
    event_filter = Or(diagnosis_filter, medication_filter)
    filtered_events, filtered_visits = trace_helper.apply_event_filters(
        trace_events, event_filter, running_meds, end_meds, diagnoses)

    assert filtered_visits == {'someVisit'}
    assert len(filtered_visits) == 1
    assert filtered_events is not None
    assert len(filtered_events) == 2
