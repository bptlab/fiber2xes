import csv
import os
import re

dirname = os.path.dirname(__file__)
ABSTRACTION_VOCAB_PATH = os.path.join(dirname, "abstraction_high_level.csv")


class Abstraction(object):

    def get_abstract_event_name(event_name, remove_unlisted=False, exact_match=False, delimiter=";"):
        # Returns abstract name if possible and remove unlisted entries
        table = csv.reader(open(ABSTRACTION_VOCAB_PATH), delimiter=delimiter)

        abstraction_names = next(table)
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
