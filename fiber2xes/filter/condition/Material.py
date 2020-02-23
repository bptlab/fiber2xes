class Material(object):
    """Filter for a specific material given by the code. Event and Trace filtering are possible.

    Keyword arguments:
    material_code -- the material code
    """

    def __init__(self, material_code):
        self.material_code = material_code

    def is_relevant_trace(self, trace):
        """Checks if the trace contains the material at least once.

        Keyword arguments:
        trace -- the trace
        """
        for event in trace:
            if self.is_relevant_event(event):
                return True
        return False

    def is_relevant_event(self, event):
        """Checks if the event is this kind of material

        Keyword arguments:
        event -- the event
        """
        return event.context_material_code == self.material_code
