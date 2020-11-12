"""
Defines an abstract Filter class that has to be implemented by concrete filters
"""

from abc import ABC, abstractmethod


class Filter(ABC):
    @abstractmethod
    def is_relevant_trace(self, trace) -> bool:
        pass

    @abstractmethod
    def is_relevant_event(self, event) -> bool:
        pass
