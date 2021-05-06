"""
Extension of diagnosis class
"""
from fiber.condition.fact.fact import _FactCondition  # type: ignore
from fiber.database.table import (  # type: ignore
    d_pers,
    fact,
    fd_diag,
)


class DiagnosisWithTime(_FactCondition):
    """
    This is an extension of the Diagnosis Class to also contain time of day-keys.
    """
    dimensions = {'DIAGNOSIS'}
    d_table = fd_diag
    code_column = fd_diag.CONTEXT_DIAGNOSIS_CODE
    category_column = fd_diag.DIAGNOSIS_TYPE
    description_column = fd_diag.DESCRIPTION

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_table.CONTEXT_NAME,
        fact.TIME_OF_DAY_KEY,
        fact.CAREGIVER_GROUP_KEY,
        fact.FACILITY_KEY,
        fact.META_DATA_KEY,
        description_column,
        code_column,
        fact.ENCOUNTER_KEY,
    ]
