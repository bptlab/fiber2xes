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
cohort_filter = Diagnosis("M54.5", "ICD-10")
cohort_size = 5
trace_type = "mrn"
trace_filter = DiagnosisFilter("M54.5")
# CONFIG END


dateTimeObj = datetime.now()
timestampStr = dateTimeObj.strftime("%Y%m%d-%H%M%S")
cohort = Cohort(cohort_filter, limit=cohort_size)
print(f'{len(cohort.mrns())} patients in the cohort')

event_log = cohort_to_event_log(cohort, trace_type, trace_filter=trace_filter, remove_unlisted=True, verbose=False, remove_duplicates=True)

save_event_log_to_file(event_log, file_path="./xes-logs/log_" + timestampStr + "_" + trace_type + "_" + str(cohort_size) + "-2.xes")
