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
