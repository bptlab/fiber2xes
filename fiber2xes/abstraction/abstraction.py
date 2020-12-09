"""
Module containing the function to abstract an event name based on an abstraction table
"""

import csv
import re

from typing import Optional


def get_abstract_event_name(abstraction_path: str, abstraction_exact_match: bool,
                            abstraction_delimiter: str, event_name: Optional[str],
                            remove_unlisted: bool = False, anamnesis: bool = False,
                            include_anamnesis_events: bool = True) -> Optional[str]:
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
    anamnesis -- is the passed event an anamnesis event
    include_anamnesis_events -- should anamnesis events be included in the xes log
    """
    if event_name is None:
        return None

    if abstraction_path is None:
        if anamnesis:
            return 'Anamnesis: ' + event_name
        return event_name

    # Returns abstract name if possible and remove unlisted entries
    # If the event is an anamnesis event, "Anamnesis: " will be added in front of
    # the abstracted event_name or the event will be discarded.
    # It depends on the choice the user made.
    table = csv.reader(open(abstraction_path), delimiter=abstraction_delimiter)

    abstraction_names = next(table)
    for row in table:
        for i, entry in enumerate(row):
            if entry and ((not abstraction_exact_match and
                           re.search(str(entry), str(event_name), re.IGNORECASE) is not None) or
                          (abstraction_exact_match and
                           str(entry).lower() == str(event_name).lower())):
                if abstraction_names[i].lower() == "group":
                    if anamnesis and include_anamnesis_events:
                        return 'Anamnesis: ' + entry
                    if anamnesis and not include_anamnesis_events:
                        return None
                    return entry
                if abstraction_names[i].lower() == "blacklist":
                    return None
                if abstraction_names[i].lower() == "whitelist":
                    if anamnesis and include_anamnesis_events:
                        return 'Anamnesis: ' + entry
                    return entry
                if anamnesis and include_anamnesis_events:
                    return 'Anamnesis: ' + abstraction_names[i]
                if anamnesis and not include_anamnesis_events:
                    return None
                return abstraction_names[i]

    if remove_unlisted:
        return None
    if anamnesis and include_anamnesis_events:
        return 'Anamnesis: ' + event_name
    return event_name
