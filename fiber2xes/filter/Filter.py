"""
Defines an abstract Filter class that has to be implemented by concrete filters
"""

from abc import ABC, abstractmethod


class Filter(ABC):
    """
    A generic filter class
    """
    @abstractmethod
    def is_relevant_trace(self, trace) -> bool:
        """
        The abstract method which determines whether a trace is relevant
        """
        pass

    @abstractmethod
    def is_relevant_event(self, event) -> bool:
        """
        The abstract method which determines whether an event is relevant
        """
        pass
