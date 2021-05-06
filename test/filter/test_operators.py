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

procedure_event_a = MockEvent(context_procedure_code='SomeProcedureCode')

procedure_event_b = MockEvent(context_procedure_code='OtherProcedureCode')

diagnosis_event_a = MockEvent(context_diagnosis_code='SomeDiagnosisCode')

diagnosis_event_b = MockEvent(context_diagnosis_code='OtherDiagnosisCode')

material_event_a = MockEvent(context_material_code='SomeMaterialCode')

material_event_b = MockEvent(context_material_code='OtherMaterialCode')

unknown_event = MockEvent()

not_applicable_event = MockEvent(context_procedure_code='MSDW_NOT APPLICABLE',
                                 context_diagnosis_code='MSDW_NOT APPLICABLE',
                                 context_material_code='MSDW_NOT APPLICABLE')

timestamp_a = "2020-11-4 08:11:00"


def test_and_event():
    unknown_diagnosis = Diagnosis("MSDW_UNKNOWN")
    known_procedure = Procedure("SomeProcedureCode")

    condition = And(unknown_diagnosis, known_procedure)

    assert condition.is_relevant_event(procedure_event_a)
    assert not condition.is_relevant_event(procedure_event_b)
    assert not condition.is_relevant_event(diagnosis_event_a)
    assert not condition.is_relevant_event(diagnosis_event_b)
    assert not condition.is_relevant_event(material_event_a)
    assert not condition.is_relevant_event(material_event_b)
    assert not condition.is_relevant_event(unknown_event)
    assert not condition.is_relevant_event(not_applicable_event)


def test_and_trace():
    known_diagnosis = Diagnosis("SomeDiagnosisCode")
    known_procedure = Procedure("SomeProcedureCode")

    condition = And(known_diagnosis, known_procedure)

    trace = [procedure_event_a, diagnosis_event_b,
             procedure_event_b, diagnosis_event_a]
    assert condition.is_relevant_trace(trace)

    other_trace = [procedure_event_a, procedure_event_b, diagnosis_event_b,
                   material_event_a, material_event_b, unknown_event, not_applicable_event]
    assert not condition.is_relevant_trace(other_trace)


def test_or_event():
    known_diagnosis = Diagnosis("SomeDiagnosisCode")
    known_material = Material("SomeMaterialCode")

    condition = Or(known_material, known_diagnosis)

    assert condition.is_relevant_event(diagnosis_event_a)
    assert condition.is_relevant_event(material_event_a)
    assert not condition.is_relevant_event(procedure_event_a)
    assert not condition.is_relevant_event(diagnosis_event_b)
    assert not condition.is_relevant_event(procedure_event_b)
    assert not condition.is_relevant_event(material_event_b)
    assert not condition.is_relevant_event(unknown_event)
    assert not condition.is_relevant_event(not_applicable_event)


def test_or_trace():
    known_diagnosis = Diagnosis("SomeDiagnosisCode")
    known_procedure = Procedure("SomeProcedureCode")

    condition = Or(known_diagnosis, known_procedure)

    trace = [procedure_event_a, diagnosis_event_b,
             procedure_event_b, unknown_event]
    assert condition.is_relevant_trace(trace)

    other_trace = [procedure_event_b, diagnosis_event_b,
                   material_event_a, material_event_b, unknown_event, not_applicable_event]
    assert not condition.is_relevant_trace(other_trace)


def test_not_event():
    known_procedure = Procedure("OtherProcedureCode")

    condition = Not(known_procedure)

    assert condition.is_relevant_event(procedure_event_a)
    assert not condition.is_relevant_event(procedure_event_b)
    assert condition.is_relevant_event(diagnosis_event_a)
    assert condition.is_relevant_event(diagnosis_event_b)
    assert condition.is_relevant_event(material_event_a)
    assert condition.is_relevant_event(material_event_b)
    assert condition.is_relevant_event(unknown_event)
    assert condition.is_relevant_event(not_applicable_event)


def test_not_trace():
    known_procedure = Procedure("OtherProcedureCode")

    condition = Not(known_procedure)

    trace = [procedure_event_b]
    assert not condition.is_relevant_trace(trace)

    other_trace = [diagnosis_event_a, diagnosis_event_b, material_event_a,
                   material_event_b, procedure_event_a, not_applicable_event, unknown_event]
    assert condition.is_relevant_trace(other_trace)
