"""
Module containing the function to abstract an event name based on an abstraction table
"""

import csv
import re


def get_abstract_event_name(abstraction_path, abstraction_exact_match,
                            abstraction_delimiter, event_name, remove_unlisted=False,
                            anamnesis=False, anamnesis_events='all'):
    """
    Abstracts an event name to an abstract activity identifier according to a
    given abstraction table.

    Keyword arguments:
    abstraction_path -- path to the abstraction table stored as a .csv-file
    abstraction_delimiter -- column delimiter used in abstraction table
    abstraction_exact_match -- flag to match only keywords that are identical to
                               the given event name
    event_name -- name of the event that should be abstracted
    remove_unlisted -- flag to remove all events that are not included in the abstraction table
    anamnesis -- is the passed event an anamnese event
    anamnesis_events -- which anamnesis events should be included in the xes log
    """
    if abstraction_path is None:
        if anamnesis:
            return 'Anamnesis: ' + event_name
        return event_name

    # Returns abstract name if possible and remove unlisted entries
    # If the event is an anamnesis event, "Anamnesis: " will be added before the abstracted event_name
    table = csv.reader(open(abstraction_path), delimiter=abstraction_delimiter)

    abstraction_names = next(table)
    for row in table:
        for i, entry in enumerate(row):
            if entry and ((not abstraction_exact_match and \
                re.search(str(entry), str(event_name), re.IGNORECASE) is not None) or \
                (abstraction_exact_match and str(entry).lower() == str(event_name).lower())):
                if abstraction_names[i].lower() == "group":
                    if anamnesis and anamnesis_events != 'none':
                        return 'Anamnesis: ' + entry
                    return entry
                if abstraction_names[i].lower() == "blacklist":
                    return None
                if abstraction_names[i].lower() == "whitelist":
                    if anamnesis and anamnesis_events != 'none':
                        return 'Anamnesis: ' + entry
                    return event_name
                if anamnesis and anamnesis_events != 'none':
                    return 'Anamnesis: ' + abstraction_names[i]
                return abstraction_names[i]

    if remove_unlisted and not (anamnesis and anamnesis_events == 'all'):
        return None
    elif anamnesis and anamnesis_events == 'all':
        return 'Anamnesis: ' + event_name
    return event_name
