"""
Defines Or operator
"""


class Or():
    """It uses two filters and check for both if the trace or event is relevant.
    Returns true if at least one filter is true.

    Keyword arguments:
    filter1 -- the first filter
    filter2 -- the second filter
    """

    def __init__(self, filter1, filter2):
        self.filter1 = filter1
        self.filter2 = filter2

    def is_relevant_trace(self, trace) -> bool:
        """Applies both filters on the trace and returns true if at least one returned true

        Keyword arguments:
        trace -- the trace
        """
        return self.filter1.is_relevant_trace(trace) or self.filter2.is_relevant_trace(trace)

    def is_relevant_event(self, event) -> bool:
        """Applies both filters on the event and returns true if at least one returned true

        Keyword arguments:
        event -- the event
        """
        return self.filter1.is_relevant_event(event) or self.filter2.is_relevant_event(event)
