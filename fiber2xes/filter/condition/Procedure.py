class Procedure(object):
    """Filter for a specific procedure given by the code. Event and Trace filtering are possible.

    Keyword arguments:
    procedure_code -- the procedure code
    """

    def __init__(self, procedure_code):
        self.procedure_code = procedure_code

    def is_relevant_trace(self, trace):
        """Checks if the trace contains the procedure at least once.

        Keyword arguments:
        trace -- the trace
        """
        for event in trace:
            if self.is_relevant_event(event):
                return True
        return False

    def is_relevant_event(self, event):
        """Checks if the event is this kind of procedure

        Keyword arguments:
        event -- the event
        """
        return event.context_procedure_code == self.procedure_code
