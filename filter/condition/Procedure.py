class Procedure(object):
    def __init__(self, procedure_code):
        self.procedure_code = procedure_code

    def is_relevant_trace(self, trace):
        for event in trace:
            if self.is_relevant_event(event):
                return True
        return False

    def is_relevant_event(self, event):
        return event.context_procedure_code == self.procedure_code
