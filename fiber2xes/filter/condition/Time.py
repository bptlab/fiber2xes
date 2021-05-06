"""
Defines functions to check events for time condition
"""
from datetime import datetime
from fiber2xes.filter.Filter import Filter

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class Time(Filter):
    """
    Filter the traces based on timing conditions (see parameter).
    Event filtering is not possible.

    Keyword arguments:
    one_event_after -- The trace is relevant if one event of the trace was after
                       the given date (default datetime.min)
    one_event_before -- The trace is relevant if one event of the trace was before
                        the given date (default datetime.max)
    all_events_after -- The trace is relevant if all events of the are were after
                        the given date (default datetime.min)
    all_events_before -- The trace is relevant if all events of the are were after
                         the given date (default datetime.max)
    """

    def __init__(self, one_event_after=None, one_event_before=None,
                 all_events_after=None, all_events_before=None):
        if one_event_after is None:
            self.one_event_after = datetime.min
        else:
            self.one_event_after = datetime.strptime(
                one_event_after, DATETIME_FORMAT)

        if one_event_before is None:
            self.one_event_before = datetime.max
        else:
            self.one_event_before = datetime.strptime(
                one_event_before, DATETIME_FORMAT)

        if all_events_after is None:
            self.all_events_after = datetime.min
        else:
            self.all_events_after = datetime.strptime(
                all_events_after, DATETIME_FORMAT)

        if all_events_before is None:
            self.all_events_before = datetime.max
        else:
            self.all_events_before = datetime.strptime(
                all_events_before, DATETIME_FORMAT)

    def is_relevant_trace(self, trace) -> bool:
        """Checks if the trace is relevant based on the given timing parameters

        Keyword arguments:
        trace -- the trace
        """
        one_before = False
        one_after = False
        for event in trace:
            timestamp_as_datetime = event.timestamp
            if timestamp_as_datetime > self.one_event_after:
                one_after = True
            if timestamp_as_datetime < self.one_event_before:
                one_before = True
            if timestamp_as_datetime > self.all_events_before:
                return False
            if timestamp_as_datetime < self.all_events_after:
                return False
        return one_after and one_before

    def is_relevant_event(self, event) -> bool:
        """is_relevant_event is not defined and raises an expection.

        Keyword arguments:
        event -- the event
        """
        raise Exception("Time is not implemented for events but for traces")
