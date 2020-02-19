from datetime import datetime

datetime_format = "%Y-%m-%d %H:%M:%S"


class Time(object):
    def __init__(self, one_event_after=None, one_event_before=None, all_events_after=None, all_events_before=None):
        if one_event_after is None:
            self.one_event_after = datetime.min
        else:
            self.one_event_after = datetime.strptime(one_event_after, datetime_format)

        if one_event_before is None:
            self.one_event_before = datetime.max
        else:
            self.one_event_before = datetime.strptime(one_event_before, datetime_format)

        if all_events_after is None:
            self.all_events_after = datetime.min
        else:
            self.all_events_after = datetime.strptime(all_events_after, datetime_format)

        if all_events_before is None:
            self.all_events_before = datetime.max
        else:
            self.all_events_before = datetime.strptime(all_events_before, datetime_format)

    def is_relevant_trace(self, trace):
        one_before = False
        one_after = False
        for event in trace:
            timestamp_as_datetime = event.timestamp.to_pydatetime()
            if timestamp_as_datetime > self.one_event_after:
                one_after = True
            if timestamp_as_datetime < self.one_event_before:
                one_before = True
            if timestamp_as_datetime > self.all_events_before:
                return False
            if timestamp_as_datetime < self.all_events_after:
                return False
        return one_after and one_before

    def is_relevant_event(self, event):
        raise Exception("Time is not implemented for events but for traces")
