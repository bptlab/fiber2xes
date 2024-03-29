"""
Provides the two main functions of fiber2xes for creation and serialisation of event logs
out of fiber
"""

from .fiber2xes import cohort_to_event_log
from .xesfactory import save_event_log_to_file

__all__ = [
    'cohort_to_event_log',
    'save_event_log_to_file'
]
