class Diagnosis(object):
    def __init__(self, diagnosis_code):
        self.diagnosis_code = diagnosis_code
    
    def is_relevant_trace(self, trace):
        for event in trace:
            if self.is_relevant_event(event):
                return True
        
        return False
    
    def is_relevant_event(self, event):
        return event.context_diagnosis_code == self.diagnosis_code