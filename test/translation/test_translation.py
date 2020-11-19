from fiber2xes.translation.translator import Translator

from mocks.MockEvent import MockEvent

vocabulary_path = "test/test_translation_vocabulary.csv"

translator = Translator()

procedure_event = MockEvent(context_procedure_code='SomeProcedureCode')

diagnosis_event = MockEvent(context_diagnosis_code='SomeDiagnosisCode')

material_event = MockEvent(context_material_code='SomeMaterialCode')

unknown_event = MockEvent()

not_applicable_event = MockEvent(context_procedure_code='MSDW_NOT APPLICABLE',
                                 context_diagnosis_code='MSDW_NOT APPLICABLE',
                                 context_material_code='MSDW_NOT APPLICABLE')


def test_is_event_procedure():
    assert translator.is_event_procedure(procedure_event)

    assert not translator.is_event_procedure(unknown_event)

    assert not translator.is_event_procedure(diagnosis_event)

    assert not translator.is_event_procedure(material_event)

    assert not translator.is_event_procedure(not_applicable_event)


def test_is_event_diagnosis():
    assert translator.is_event_diagnosis(diagnosis_event)

    assert not translator.is_event_diagnosis(unknown_event)

    assert not translator.is_event_diagnosis(procedure_event)

    assert not translator.is_event_diagnosis(material_event)

    assert not translator.is_event_diagnosis(not_applicable_event)


def test_is_event_material():
    assert translator.is_event_material(material_event)

    assert not translator.is_event_material(unknown_event)

    assert not translator.is_event_material(procedure_event)

    assert not translator.is_event_material(diagnosis_event)

    assert not translator.is_event_material(not_applicable_event)


def test_is_known_event():
    assert translator.is_known_event(material_event)
    assert translator.is_known_event(procedure_event)
    assert translator.is_known_event(diagnosis_event)

    assert not translator.is_known_event(unknown_event)
    assert not translator.is_known_event(not_applicable_event)


def test_vocabulary_lookup():

    termAA = translator.vocabulary_lookup(
        vocabulary_path=vocabulary_path,
        search_term="termA",
        search_column=0,
        target_column=1)

    termAB = translator.vocabulary_lookup(
        vocabulary_path=vocabulary_path,
        search_term="termB",
        search_column=0,
        target_column=1)

    termBA = translator.vocabulary_lookup(
        vocabulary_path=vocabulary_path,
        search_term="termA",
        search_column=0,
        target_column=2)

    termBB = translator.vocabulary_lookup(
        vocabulary_path=vocabulary_path,
        search_term="termB",
        search_column=0,
        target_column=2)

    termC = translator.vocabulary_lookup(
        vocabulary_path=vocabulary_path,
        search_term="termC",
        search_column=0,
        target_column=2)

    termNoTargetColumn = translator.vocabulary_lookup(
        vocabulary_path=vocabulary_path,
        search_term="termB",
        search_column=0,
        target_column=333)

    termNoSearchColumn = translator.vocabulary_lookup(
        vocabulary_path=vocabulary_path,
        search_term="termB",
        search_column=999,
        target_column=1)

    assert termC == None
    assert termNoTargetColumn == None
    assert termNoSearchColumn == None
    assert termAA == 'translationAA'
    assert termAB == 'translationAB'
    assert termBB == 'translationBB'
    assert termBA == 'translationBA'


def test_translate_procedure():

    event_context_a, translation_a = translator.translate_procedure(
        [], "some_code", verbose=False)

    assert event_context_a == "UNKNOWN"
    assert translation_a == None

    event_context_b, translation_b = translator.translate_procedure(
        ["ICD-10"], "M54.5", verbose=False)
    assert event_context_b == "ICD-10"
    assert translation_b == 'Low back pain'

    event_context_c, translation_c = translator.translate_procedure(
        ["SYSTEM"], "M54.5", verbose=False)
    assert event_context_c == "SYSTEM"
    assert translation_c == None

    event_context_d, translation_d = translator.translate_procedure(
        ["IMO"], "M54.5", verbose=False)
    assert event_context_d == "IMO"
    assert translation_d == None

    event_context_e, translation_e = translator.translate_procedure(
        ["EPIC"], "M54.5", verbose=False)
    assert event_context_e == "EPIC"
    assert translation_e == None


def test_translate_diagnosis():
    event_context_a, translation_a = translator.translate_diagnosis(
        [], "some_code", verbose=False)

    assert event_context_a == "UNKNOWN"
    assert translation_a == None

    event_context_b, translation_b = translator.translate_diagnosis(
        ["ICD-10"], "M54.5", verbose=False)
    assert event_context_b == "ICD-10"
    assert translation_b == 'Low back pain'

    event_context_c, translation_c = translator.translate_diagnosis(
        ["SYSTEM"], "M54.5", verbose=False)
    assert event_context_c == "SYSTEM"
    assert translation_c == None

    event_context_d, translation_d = translator.translate_diagnosis(
        ["IMO"], "M54.5", verbose=False)
    assert event_context_d == "IMO"
    assert translation_d == None

    event_context_e, translation_e = translator.translate_diagnosis(
        ["EPIC"], "M54.5", verbose=False)
    assert event_context_e == "EPIC"
    assert translation_e == None


def test_translate_material():
    event_context_a, translation_a = translator.translate_material(
        [], verbose=False)

    assert event_context_a == "UNKNOWN"
    assert translation_a == None

    event_context_b, translation_b = translator.translate_material(
        ["EPIC MEDICATION"], verbose=False)
    assert event_context_b == "EPIC MEDICATION"
    assert translation_b == None


def test_identify_consultation():
    none_description = translator.identify_consultation(None)
    assert none_description is None

    wrong_description = translator.identify_consultation(
        "some_other_description")
    assert wrong_description is None

    description = translator.identify_consultation(
        "consult to some specialist")
    assert description == "some specialist"


def test_translate_procedure_to_event():
    procedure_event_pain_score = MockEvent(context_procedure_code='SomeProcedureCode',
                                           procedure_description="PAIN SCORE", value="3")

    procedure_event_consultation = MockEvent(context_procedure_code='SomeProcedureCode',
                                             procedure_description="Consult To Some Specialist")
    procedure_event_anamnesis = MockEvent(context_procedure_code='SomeProcedureCode',
                                          procedure_description="SomeDescription", level3_action_name="Reported something")

    procedure_event = MockEvent(context_procedure_code="S99.912A", context_name_1="ICD-10", context_name_2="ICD-10",
                                context_name="ICD-10", procedure_description="SomeDescription")

    event_name_1, event_description_1, event_type_1, anamnesis_1, event_context_1, event_code_1 = translator.translate_to_event(
        procedure_event_pain_score, False)
    assert event_name_1 == 'PAIN SCORE'
    assert event_description_1 == 'PAIN SCORE: 3'
    assert event_type_1 == 'PROCEDURE'
    assert anamnesis_1 == False
    assert event_context_1 == "UNKNOWN"
    assert event_code_1 == "SomeProcedureCode"

    event_name_2, event_description_2, event_type_2, anamnesis_2, event_context_2, event_code_2 = translator.translate_to_event(
        procedure_event_consultation, False)
    assert event_name_2 == 'Some Specialist'
    assert event_description_2 == 'Consult To Some Specialist'
    assert event_type_2 == 'CONSULTATION'
    assert anamnesis_2 == False
    assert event_context_2 == "UNKNOWN"
    assert event_code_2 == "SomeProcedureCode"

    event_name_3, event_description_3, event_type_3, anamnesis_3, event_context_3, event_code_3 = translator.translate_to_event(
        procedure_event_anamnesis, False)
    assert event_name_3 == 'SomeDescription'
    assert event_description_3 == 'SomeDescription'
    assert event_type_3 == 'PROCEDURE'
    assert anamnesis_3 == True
    assert event_context_3 == "UNKNOWN"
    assert event_code_3 == "SomeProcedureCode"

    event_name_4, event_description_4, event_type_4, anamnesis_4, event_context_4, event_code_4 = translator.translate_to_event(
        procedure_event, False)
    assert event_name_4 == 'Unspecified injury of left ankle, initial encounter'
    assert event_description_4 == 'SomeDescription'
    assert event_type_4 == 'PROCEDURE'
    assert anamnesis_4 == False
    assert event_context_4 == "ICD-10"
    assert event_code_4 == "S99.912A"


def test_translate_diagnosis_to_event():
    diagnosis_event_anamnesis = MockEvent(context_diagnosis_code='SomeDiagnosisCode',
                                          description="SomeDescription", level3_action_name="Reported something")

    diagnosis_event = MockEvent(context_diagnosis_code='S99.912A', context_name_1="ICD-10", context_name_2="ICD-10",
                                context_name="ICD-10", description="SomeDescription")

    diagnosis_event_2 = MockEvent(context_diagnosis_code='SomeDiagnosisCode', context_name_1="ICD-10", context_name_2="ICD-10",
                                  context_name="ICD-10", description="SomeDescription")

    event_name_1, event_description_1, event_type_1, anamnesis_1, event_context_1, event_code_1 = translator.translate_to_event(
        diagnosis_event_anamnesis, False)
    assert event_name_1 == 'SomeDescription'
    assert event_description_1 == 'SomeDescription'
    assert event_type_1 == 'DIAGNOSIS'
    assert anamnesis_1 == True
    assert event_context_1 == "UNKNOWN"
    assert event_code_1 == "SomeDiagnosisCode"

    event_name_2, event_description_2, event_type_2, anamnesis_2, event_context_2, event_code_2 = translator.translate_to_event(
        diagnosis_event, False)
    assert event_name_2 == 'Unspecified injury of left ankle, initial encounter'
    assert event_description_2 == 'SomeDescription'
    assert event_type_2 == 'DIAGNOSIS'
    assert anamnesis_2 == False
    assert event_context_2 == "ICD-10"
    assert event_code_2 == "S99.912A"

    event_name_3, event_description_3, event_type_3, anamnesis_3, event_context_3, event_code_3 = translator.translate_to_event(
        diagnosis_event_2, False)
    assert event_name_3 == 'SomeDescription'
    assert event_description_3 == 'SomeDescription'
    assert event_type_3 == 'DIAGNOSIS'
    assert anamnesis_3 == False
    assert event_context_3 == "ICD-10"
    assert event_code_3 == "SomeDiagnosisCode"


def test_translate_material_to_event():
    material_event = MockEvent(context_material_code='SomeMaterialCode',
                               material_name="SomeMaterialName")
    material_anamnesis = MockEvent(context_material_code='SomeMaterialCode',
                                   material_name="SomeMaterialName", level3_action_name="Reported some material")

    event_name_1, event_description_1, event_type_1, anamnesis_1, event_context_1, event_code_1 = translator.translate_to_event(
        material_event, False)
    assert event_name_1 == 'SomeMaterialName'
    assert event_description_1 == 'SomeMaterialName'
    assert event_type_1 == 'MATERIAL'
    assert anamnesis_1 == False
    assert event_context_1 == "UNKNOWN"
    assert event_code_1 == "SomeMaterialCode"

    event_name_2, event_description_2, event_type_2, anamnesis_2, event_context_2, event_code_2 = translator.translate_to_event(
        material_anamnesis, False)
    assert event_name_2 == 'SomeMaterialName'
    assert event_description_2 == 'SomeMaterialName'
    assert event_type_2 == 'MATERIAL'
    assert anamnesis_2 == True
    assert event_context_2 == "UNKNOWN"
    assert event_code_2 == "SomeMaterialCode"
