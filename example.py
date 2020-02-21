import sys
from datetime import datetime

from fiber import Cohort
from fiber.condition import (
    Diagnosis,
)

from fiber2xes import (
    cohort_to_event_log,
    save_event_log_to_file
)
from fiber2xes.filter.condition import Diagnosis as DiagnosisFilter

# CONFIG START
cohort_filter = Diagnosis("M54.5", "ICD-10")  # Fiber filter (See fiber documentation)
cohort_size = 5  # Number of patients in the cohort
trace_type = "mrn"  # Trace type should be either "mrn" or "visit"
trace_filter = DiagnosisFilter("M54.5")  # Filter for traces in XES log
# CONFIG END

# Query cohort of patients from database
cohort = Cohort(cohort_filter, limit=cohort_size)
print(f'{len(cohort.mrns())} patients in the cohort')

# Create XES event log with fiber2xes
event_log = cohort_to_event_log(
  cohort,
  trace_type,
  trace_filter=trace_filter,
  remove_unlisted=True,
  verbose=False,
  remove_duplicates=True,
  abstraction_path="./abstraction-tables/abstraction_high_level.csv",
)

# Write event log to file
dateTimeObj = datetime.now()
timeStr = dateTimeObj.strftime("%Y%m%d-%H%M%S")
save_event_log_to_file(event_log, file_path="./log_" + timeStr + "_" + trace_type + "_" + str(cohort_size) + ".xes")
