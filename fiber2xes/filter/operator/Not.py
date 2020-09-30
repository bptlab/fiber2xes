"""
Defines Not operator
"""


class Not():
    """It takes on filter and inverts its result for the given event or trace

    Keyword arguments:
    filter -- the filter
    """

    def __init__(self, filter1):
        self.filter = filter1

    def is_relevant_trace(self, trace):
        """Applies the filter on the trace and returns the inverted result

        Keyword arguments:
        trace -- the trace
        """
        return not self.filter.is_relevant_trace(trace)

    def is_relevant_event(self, event):
        """Applies the filter on the event and returns the inverted result

        Keyword arguments:
        event -- the event
        """
        return not self.filter.is_relevant_event(event)
