class Or(object):
    def __init__(self, filter1, filter2):
        self.filter1 = filter1
        self.filter2 = filter2
    
    def is_relevant_trace(trace):
        return self.filter1.is_relevant_trace(trace) or self.filter2.is_relevant_trace(trace)
    
    def is_relevant_event(event):
        return self.filter1.is_relevant_event(event) or self.filter2.is_relevant_event(event)
