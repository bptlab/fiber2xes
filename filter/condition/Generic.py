class Generic(object):
    def __init__(self, lambda_expression):
        self.lambda_expression = lambda_expression
    
    def is_relevant_trace(self, trace):
        return self.lambda_expression(trace)
    
    def is_relevant_event(self, event):
        return self.lambda_expression(event)