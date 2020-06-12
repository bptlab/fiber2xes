from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    fd_proc,
)


class ProcedureWithTime(_FactCondition):
    """
    This is an extension of the Procedure Class, to also contain time of day-keys.
    """
    dimensions = {'PROCEDURE'}
    d_table = fd_proc
    code_column = fd_proc.CONTEXT_PROCEDURE_CODE
    category_column = fd_proc.PROCEDURE_TYPE
    description_column = fd_proc.PROCEDURE_DESCRIPTION

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
