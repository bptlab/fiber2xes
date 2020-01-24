import csv
import re

abstraction_names = None
table = None
remove_unlisted = False
exact_match = False


def prepare(abstraction_csv_path, delimiter, _remove_unlisted, _exact_match):
    global table, abstraction_names, remove_unlisted, exact_match
    if table is None or abstraction_names is None:
        table_reader = csv.reader(open(abstraction_csv_path), delimiter=delimiter)
        abstraction_names = next(table_reader)
        table = list(table_reader)
    remove_unlisted = _remove_unlisted
    exact_match = _exact_match


def get_abstract_event_name(event_name):
    global table, abstraction_names, remove_unlisted, exact_match
    for row in table:
        for i, entry in enumerate(row):
            if entry and ((not exact_match and re.search(str(entry), str(event_name), re.IGNORECASE) is not None)
               or (exact_match and str(entry).lower() == str(event_name).lower())):
                if abstraction_names[i].lower() == "group":
                    return entry
                elif abstraction_names[i].lower() == "blacklist":
                    return None
                elif abstraction_names[i].lower() == "whitelist":
                    return event_name
                return abstraction_names[i]

    if remove_unlisted:
        return None
    return event_name
