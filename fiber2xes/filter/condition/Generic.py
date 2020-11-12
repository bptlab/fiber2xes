"""
Defines functions to check events for generic condition
"""

from typing import Callable


class Generic():
    """Filter the traces or events with the given lambda expression.
    The lambda expression gets the trace or event as a parameter and it should return true or false.
    In case of true its a relevant trace or event, otherwise not.

    Keyword arguments:
    lambda_expression -- the lambda expression which will be applied on all traces and events
    """

    def __init__(self, lambda_expression: Callable):
        self.lambda_expression = lambda_expression

    def is_relevant_trace(self, trace) -> bool:
        """Returns the value of the lambda expression applied on the trace

        Keyword arguments:
        trace -- the trace
        """
        return self.lambda_expression(trace)

    def is_relevant_event(self, event) -> bool:
        """Returns the value of the lambda expression applied on the event

        Keyword arguments:
        event -- the event
        """
        return self.lambda_expression(event)
