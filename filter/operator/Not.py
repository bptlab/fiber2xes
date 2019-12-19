class Not(object):
    def __init__(self, filter1):
        self.filter1 = filter1
    
    def is_relevant_trace(trace):
        return not self.filter1.is_relevant_trace(trace)
    
    def is_relevant_event(event):
        return not self.filter1.is_relevant_event(event)
