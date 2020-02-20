import csv
import re


class Abstraction(object):

    def get_abstract_event_name(abstraction_path, abstraction_exact_match, abstraction_delimiter, event_name, remove_unlisted=False):
        if abstraction_path is None:
            return event_name

        # Returns abstract name if possible and remove unlisted entries
        table = csv.reader(open(abstraction_path), delimiter=abstraction_delimiter)

        abstraction_names = next(table)
        for row in table:
            for i, entry in enumerate(row):
                if entry and ((not abstraction_exact_match and re.search(str(entry), str(event_name), re.IGNORECASE) is not None)
                   or (abstraction_exact_match and str(entry).lower() == str(event_name).lower())):
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
