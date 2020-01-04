class Material(object):
    def __init__(self, material_code):
        self.material_code = material_code
    
    def is_relevant_trace(self, trace):
        for event in trace:
            if self.is_relevant_event(event):
                return True
        return False
    
    def is_relevant_event(self, event):
        return event.context_material_code == self.material_code
