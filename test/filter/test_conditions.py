from fiber2xes.filter.condition import (
    Diagnosis,
    Generic,
    Material,
    Procedure,
    Time
)

from mocks.MockEvent import MockEvent

procedure_event_a = MockEvent(context_procedure_code='SomeCode')

procedure_event_b = MockEvent(context_procedure_code='OtherCode')

diagnosis_event_a = MockEvent(context_diagnosis_code='SomeCode')

diagnosis_event_b = MockEvent(context_diagnosis_code='OtherCode')

material_event_a = MockEvent(context_material_code='SomeCode')

material_event_b = MockEvent(context_material_code='OtherCode')

unknown_event = MockEvent()

not_applicable_event = MockEvent(context_procedure_code='MSDW_NOT APPLICABLE',
                                 context_diagnosis_code='MSDW_NOT APPLICABLE',
                                 context_material_code='MSDW_NOT APPLICABLE')


def test_diagnosis_event_filter():
    diagnosis_filter = Diagnosis("SomeCode")

    assert diagnosis_filter.is_relevant_event(diagnosis_event_a)
    assert not diagnosis_filter.is_relevant_event(diagnosis_event_b)
    assert not diagnosis_filter.is_relevant_event(procedure_event_a)
    assert not diagnosis_filter.is_relevant_event(procedure_event_b)
    assert not diagnosis_filter.is_relevant_event(material_event_a)
    assert not diagnosis_filter.is_relevant_event(material_event_b)
    assert not diagnosis_filter.is_relevant_event(unknown_event)
    assert not diagnosis_filter.is_relevant_event(not_applicable_event)


def test_diagnosis_trace_filter():
    diagnosis_filter = Diagnosis("SomeCode")

    trace = [diagnosis_event_a, diagnosis_event_b,
             unknown_event, not_applicable_event]
    irrelevant_trace = [procedure_event_a,
                        procedure_event_b, material_event_a, diagnosis_event_b]

    assert diagnosis_filter.is_relevant_trace(trace)
    assert not diagnosis_filter.is_relevant_trace(irrelevant_trace)


def test_generic():
    assert True


def test_material_event_filter():
    material_filter = Material("SomeCode")

    assert material_filter.is_relevant_event(material_event_a)
    assert not material_filter.is_relevant_event(material_event_b)
    assert not material_filter.is_relevant_event(procedure_event_a)
    assert not material_filter.is_relevant_event(procedure_event_b)
    assert not material_filter.is_relevant_event(diagnosis_event_a)
    assert not material_filter.is_relevant_event(diagnosis_event_b)
    assert not material_filter.is_relevant_event(unknown_event)
    assert not material_filter.is_relevant_event(not_applicable_event)


def test_material_trace_filter():
    material_filter = Material("SomeCode")

    trace = [material_event_a, material_event_b,
             unknown_event, not_applicable_event]
    irrelevant_trace = [material_event_b, diagnosis_event_a,
                        procedure_event_a, not_applicable_event]

    assert material_filter.is_relevant_trace(trace)
    assert not material_filter.is_relevant_trace(irrelevant_trace)


def test_procedure_event_filter():
    procedure_filter = Procedure("SomeCode")

    assert procedure_filter.is_relevant_event(procedure_event_a)
    assert not procedure_filter.is_relevant_event(procedure_event_b)
    assert not procedure_filter.is_relevant_event(diagnosis_event_a)
    assert not procedure_filter.is_relevant_event(diagnosis_event_b)
    assert not procedure_filter.is_relevant_event(material_event_a)
    assert not procedure_filter.is_relevant_event(material_event_b)
    assert not procedure_filter.is_relevant_event(unknown_event)
    assert not procedure_filter.is_relevant_event(not_applicable_event)


def test_procedure_trace_filter():
    procedure_filter = Procedure("SomeCode")

    trace = [procedure_event_a, procedure_event_b,
             unknown_event, not_applicable_event]
    irrelevant_trace = [procedure_event_b, diagnosis_event_a, unknown_event,
                        material_event_a, not_applicable_event]

    assert procedure_filter.is_relevant_trace(trace)
    assert not procedure_filter.is_relevant_trace(irrelevant_trace)


def test_time():

    timestamp_a = "2020-11-4 08:11:00"

    timestamp_b = "2020-12-4 08:11:00"

    event_a = MockEvent(timestamp="2020-11-4 08:12:00")
    event_b = MockEvent(timestamp="2020-11-5 08:12:00")
    event_c = MockEvent(timestamp="2020-11-6 08:12:00")
    event_d = MockEvent(timestamp="2020-11-7 08:12:00")

    one_before = Time(one_event_before=timestamp_b)
    all_before = Time(all_events_before=timestamp_b)

    one_after = Time(one_event_after=timestamp_b)
    all_after = Time(all_events_after=timestamp_b)

    assert one_before.is_relevant_trace([event_a, event_b, event_c, event_d])
    assert all_before.is_relevant_trace([event_a, event_b, event_c, event_d])

    assert not one_after.is_relevant_trace([event_a, event_b, event_c, event_d])
    assert not all_after.is_relevant_trace([event_a, event_b, event_c, event_d])

    assert True
