"""
Defines functions to check events for diagnosis condition
"""


class Diagnosis():
    """Filter for a specific diagnosis given by the code. Event and Trace filtering are possible.

    Keyword arguments:
    diagnosis_code -- the diagnosis code
    """

    def __init__(self, diagnosis_code: str):
        self.diagnosis_code = diagnosis_code

    def is_relevant_trace(self, trace) -> bool:
        """Checks if the trace contains the diagnosis at least once.

        Keyword arguments:
        trace -- the trace
        """
        for event in trace:
            if self.is_relevant_event(event):
                return True
        return False

    def is_relevant_event(self, event) -> bool:
        """Checks if the event is this kind of diagnosis

        Keyword arguments:
        event -- the event
        """
        return event.context_diagnosis_code == self.diagnosis_code
