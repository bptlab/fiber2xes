"""
Module containing the function to abstract an event name based on an abstraction table
"""

import csv
import re


def get_abstract_event_name(abstraction_path, abstraction_exact_match,
                            abstraction_delimiter, event_name, remove_unlisted=False):
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
    """
    if abstraction_path is None:
        return event_name

    # Returns abstract name if possible and remove unlisted entries
    table = csv.reader(open(abstraction_path), delimiter=abstraction_delimiter)

    abstraction_names = next(table)
    for row in table:
        for i, entry in enumerate(row):
            if entry and ((not abstraction_exact_match and \
                re.search(str(entry), str(event_name), re.IGNORECASE) is not None) or \
                (abstraction_exact_match and str(entry).lower() == str(event_name).lower())):
                if abstraction_names[i].lower() == "group":
                    return entry
                if abstraction_names[i].lower() == "blacklist":
                    return None
                if abstraction_names[i].lower() == "whitelist":
                    return event_name
                return abstraction_names[i]

    if remove_unlisted:
        return None
    return event_name
