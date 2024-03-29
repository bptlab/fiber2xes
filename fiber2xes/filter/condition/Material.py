"""
Defines functions to check events for material condition
"""
from fiber2xes.filter.Filter import Filter


class Material(Filter):
    """Filter for a specific material given by the code. Event and Trace filtering are possible.

    Keyword arguments:
    material_code -- the material code
    """

    def __init__(self, material_code: str):
        self.material_code = material_code

    def is_relevant_trace(self, trace) -> bool:
        """Checks if the trace contains the material at least once.

        Keyword arguments:
        trace -- the trace
        """
        for event in trace:
            if self.is_relevant_event(event):
                return True
        return False

    def is_relevant_event(self, event) -> bool:
        """Checks if the event is this kind of material

        Keyword arguments:
        event -- the event
        """
        return event.context_material_code == self.material_code
