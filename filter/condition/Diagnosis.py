class Diagnosis(object):
    def __init__(self, diagnosis_code, context_name, only=False):
        self.diagnosis_code = diagnosis_code
        self.context_name = context_name
        self.only = only

    def is_relevant_trace(self, trace):
        contains = False
        others = False
        for event in trace:
            if self.is_relevant_event(event):
                if self.only:
                    contains = True
                else: # it is the right event and others are allowed
                    return True
            elif Diagnosis.is_event_diagnosis(event):
                # There is an other diagnosis than the expected one
                others = True
            if contains and others:
                break # all required information received
        
        if not contains:
            return False
        if self.only and others:
            return False
        return True

    def is_relevant_event(self, event):
        return event.context_diagnosis_code == self.diagnosis_code
    
    def is_event_diagnosis(event):
        return event.context_diagnosis_code != "MSDW_NOT APPLICABLE" and event.context_diagnosis_code != "MSDW_UNKNOWN"
