from fiber2xes.abstraction.abstraction import get_abstract_event_name

abstraction_path = "../test_abstraction_table.csv"

delimiter = ","


def test_no_abstraction():
    anamnesis = get_abstract_event_name(abstraction_path=None,
                                        abstraction_exact_match=True,
                                        abstraction_delimiter=delimiter,
                                        event_name="a",
                                        anamnesis=True)
    assert anamnesis == 'Anamnesis: a'

