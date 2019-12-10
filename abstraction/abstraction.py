import csv
import os
import re

ABSTRACTION_VOCAB_PATH = os.path.join(os.path.expanduser("~"), "fiber-to-xes", "abstraction", "abstraction.csv")

class Abstraction(object):

    def get_abstract_event_name(event_name, remove_unlisted=False, delimiter=";"):
        # Returns abstract name if possible and remove unlisted entries
        table = csv.reader(open(ABSTRACTION_VOCAB_PATH), delimiter=delimiter)
        first_row = next(table)
        for row in table:
            for i, entry in enumerate(row):
                if entry and re.search(str(entry), str(event_name), re.IGNORECASE) != None:
                    if remove_unlisted and first_row[i].lower() == "Blacklist".lower():
                        return None, True
                    elif first_row[i].lower() == "Whitelist".lower():
                        return event_name, True
                    return first_row[i], False
        
        if remove_unlisted:
            return None, True
        
        return event_name, False