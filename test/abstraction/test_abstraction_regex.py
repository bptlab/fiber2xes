from fiber2xes.abstraction.abstraction import get_abstract_event_name

abstraction_path = "test/test_abstraction_table.csv"

delimiter = ","


def test_no_abstraction():
    anamnesis = get_abstract_event_name(abstraction_path=None,
                                        abstraction_exact_match=False,
                                        abstraction_delimiter=delimiter,
                                        event_name="some_event",
                                        anamnesis=True)
    assert anamnesis == 'Anamnesis: some_event'

    anamnesis_discard = get_abstract_event_name(abstraction_path=None,
                                                abstraction_exact_match=False,
                                                abstraction_delimiter=delimiter,
                                                event_name="some_event",
                                                anamnesis_events="none",
                                                anamnesis=True)
    assert anamnesis_discard == 'Anamnesis: some_event'

    no_anamenesis = get_abstract_event_name(abstraction_path=None,
                                            abstraction_exact_match=False,
                                            abstraction_delimiter=delimiter,
                                            event_name="some_event",
                                            anamnesis=False)

    assert no_anamenesis == 'some_event'


def test_group():
    group_anamnesis = get_abstract_event_name(abstraction_path=abstraction_path,
                                              abstraction_exact_match=False,
                                              abstraction_delimiter=delimiter,
                                              event_name="xyzgroupTermAxyz",
                                              anamnesis=True)
    assert group_anamnesis == 'Anamnesis: groupTermA'

    group_anamnesis_discarded = get_abstract_event_name(abstraction_path=abstraction_path,
                                                        abstraction_exact_match=False,
                                                        abstraction_delimiter=delimiter,
                                                        anamnesis_events="none",
                                                        event_name="xyzgroupTermAxyz",
                                                        anamnesis=True)
    assert group_anamnesis_discarded == 'groupTermA'  # todo: should be none

    group = get_abstract_event_name(abstraction_path=abstraction_path,
                                    abstraction_exact_match=False,
                                    abstraction_delimiter=delimiter,
                                    event_name="xyzgroupTermBxyz",
                                    anamnesis=False)
    assert group == 'groupTermB'


def test_blacklist():
    blacklisted_term = get_abstract_event_name(abstraction_path=abstraction_path,
                                               abstraction_exact_match=False,
                                               abstraction_delimiter=delimiter,
                                               event_name="xyzblacklistTermAxyz")
    assert blacklisted_term == None


def test_whitelist():
    whitelisted_term = get_abstract_event_name(abstraction_path=abstraction_path,
                                               abstraction_exact_match=False,
                                               abstraction_delimiter=delimiter,
                                               event_name="xyzwhitelistTermAxyz")

    assert whitelisted_term == 'whitelistTermA'

    whitelisted_term_anamnesis = get_abstract_event_name(abstraction_path=abstraction_path,
                                                         abstraction_exact_match=False,
                                                         abstraction_delimiter=delimiter,
                                                         anamnesis=True,
                                                         event_name="xyzwhitelistTermBxyz")

    assert whitelisted_term_anamnesis == 'Anamnesis: whitelistTermB'

    whitelisted_term_anamnesis_discarded = get_abstract_event_name(abstraction_path=abstraction_path,
                                                                   abstraction_exact_match=False,
                                                                   abstraction_delimiter=delimiter,
                                                                   anamnesis=True,
                                                                   anamnesis_events="none",
                                                                   event_name="xyzwhitelistTermBxyz")

    assert whitelisted_term_anamnesis_discarded == 'whitelistTermB'


def test_abstraction():
    abstracted_term_a = get_abstract_event_name(abstraction_path=abstraction_path,
                                                abstraction_exact_match=False,
                                                abstraction_delimiter=delimiter,
                                                event_name="xyzabstractATermAxyz")

    assert abstracted_term_a == "AbstractA"

    abstracted_anamnesis_a = get_abstract_event_name(abstraction_path=abstraction_path,
                                                     abstraction_exact_match=False,
                                                     abstraction_delimiter=delimiter,
                                                     anamnesis=True,
                                                     event_name="xyzabstractATermBxyz")

    assert abstracted_anamnesis_a == 'Anamnesis: AbstractA'

    abstracted_anamnesis_a_discarded = get_abstract_event_name(abstraction_path=abstraction_path,
                                                               abstraction_exact_match=False,
                                                               abstraction_delimiter=delimiter,
                                                               anamnesis=True,
                                                               anamnesis_events="none",
                                                               event_name="xyzabstractATermBxyz")

    assert abstracted_anamnesis_a_discarded == None

    abstracted_term_b = get_abstract_event_name(abstraction_path=abstraction_path,
                                                abstraction_exact_match=False,
                                                abstraction_delimiter=delimiter,
                                                event_name="xyzabstractBTermAxyz")

    assert abstracted_term_b == "AbstractB"

    abstracted_anamnesis_b = get_abstract_event_name(abstraction_path=abstraction_path,
                                                     abstraction_exact_match=False,
                                                     abstraction_delimiter=delimiter,
                                                     anamnesis=True,
                                                     event_name="xyzabstractBTermBxyz")

    assert abstracted_anamnesis_b == 'Anamnesis: AbstractB'

    abstracted_anamnesis_b_discarded = get_abstract_event_name(abstraction_path=abstraction_path,
                                                               abstraction_exact_match=False,
                                                               abstraction_delimiter=delimiter,
                                                               anamnesis=True,
                                                               anamnesis_events="none",
                                                               event_name="xyzabstractBTermBxyz")

    assert abstracted_anamnesis_b_discarded == None


def test_remove_unlisted():
    unlisted_term = get_abstract_event_name(abstraction_path=abstraction_path,
                                            abstraction_exact_match=False,
                                            abstraction_delimiter=delimiter,
                                            anamnesis=False,
                                            remove_unlisted=False,
                                            event_name="unlisted")

    assert unlisted_term == 'unlisted'

    unlisted_term_removed = get_abstract_event_name(abstraction_path=abstraction_path,
                                                    abstraction_exact_match=False,
                                                    abstraction_delimiter=delimiter,
                                                    anamnesis=False,
                                                    remove_unlisted=True,
                                                    event_name="unlisted")

    assert unlisted_term_removed == None

    unlisted_anamnesis = get_abstract_event_name(abstraction_path=abstraction_path,
                                                 abstraction_exact_match=False,
                                                 abstraction_delimiter=delimiter,
                                                 anamnesis=True,
                                                 remove_unlisted=False,
                                                 event_name="unlisted")

    assert unlisted_anamnesis == 'Anamnesis: unlisted'

    unlisted_anamnesis_removed = get_abstract_event_name(abstraction_path=abstraction_path,
                                                         abstraction_exact_match=False,
                                                         abstraction_delimiter=delimiter,
                                                         anamnesis=True,
                                                         anamnesis_events="none",
                                                         remove_unlisted=True,
                                                         event_name="unlisted")

    assert unlisted_anamnesis_removed == None
