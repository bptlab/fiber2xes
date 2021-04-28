"""
Provides complex deduplication logic that can be applied to events
based on seen medication and diagnoses
"""

import datetime
from typing import (Optional, Tuple)
from pyspark.sql import Row


def identify_duplicate_special_events(event, event_name: str,
                                      event_descriptor: Optional[str],
                                      day: str,
                                      seen_running_medications_per_day: dict,
                                      seen_end_medications_per_day: dict,
                                      seen_diagnosis_per_day: dict,
                                      trace_events: list,
                                      duplicate_event_identifier: Optional[str],
                                      event_identifier_to_merge: Optional[str]):
    """
    Identifies specific prescriptions or medications that have been observed multiple times per day
    """

    if ('Prescription' in event.level2_event_name or
            'Medication' in event.level2_event_name) and \
            'Anamnesis' not in event_name:

        if day not in seen_running_medications_per_day.keys():
            seen_running_medications_per_day[day] = {}
            seen_end_medications_per_day[day] = {}
        if event_descriptor not in seen_end_medications_per_day[day].keys() and \
                event.level4_field_name == 'End Date':
            seen_end_medications_per_day[day][event_descriptor] = event
        elif event_descriptor not in seen_running_medications_per_day[day].keys():
            seen_running_medications_per_day[day][event_descriptor] = event

    elif duplicate_event_identifier is not None and \
            duplicate_event_identifier in event_name:
        if day not in seen_diagnosis_per_day.keys():
            seen_diagnosis_per_day[day] = event
        elif event_identifier_to_merge is not None and \
                event_name == event_identifier_to_merge:
            new_merged_event_dict = event.asDict()
            new_merged_event_dict['timestamp'] = seen_diagnosis_per_day[day].timestamp
            new_merged_event = Row(**new_merged_event_dict)
            seen_diagnosis_per_day[day] = new_merged_event
            trace_events.append(new_merged_event)


def apply_time_lifecycle_duplicate_detection(event, event_name: str,
                                             event_descriptor: Optional[str],
                                             seen_diagnosis_per_day: dict,
                                             seen_running_medications_per_day: dict,
                                             seen_end_medications_per_day: dict,
                                             medication_list: list,
                                             duplicate_event_identifier: Optional[str]) \
        -> Tuple[str, str, bool]:
    """
    if medication related and latest seen event for this medication
    at this day, set lifecycle:transition corresponding to metadata
    else mark as duplicate
    """
    level2 = event.level2_event_name
    level4 = event.level4_field_name
    day = event.timestamp.date()
    lifecycle_state = "unknown"

    if ('Prescription' in level2 or 'Medication' in level2) and \
            'Anamnesis' not in event_name:

        if event_descriptor in seen_running_medications_per_day[day].keys():
            if event == seen_running_medications_per_day[day][event_descriptor]:
                if event_descriptor not in medication_list:
                    medication_list.append(event_descriptor)
                    lifecycle_state = "start"
                else:
                    medication_list.append(event_descriptor)
                    lifecycle_state = "resume"
            else:
                is_duplicate = True

        elif event_descriptor in seen_end_medications_per_day[day].keys():
            if event == seen_end_medications_per_day[day][event_descriptor]:
                if event_descriptor in medication_list:
                    medication_list.remove(event_descriptor)
                lifecycle_state = "complete"
            else:
                is_duplicate = True

    if duplicate_event_identifier is not None and \
            duplicate_event_identifier in event_name:
        is_duplicate = bool(event != seen_diagnosis_per_day[day])

    new_timestamp = event.timestamp
    if 'Start Date' in level4:
        new_timestamp += datetime.timedelta(seconds=1)

    return lifecycle_state, new_timestamp, is_duplicate
