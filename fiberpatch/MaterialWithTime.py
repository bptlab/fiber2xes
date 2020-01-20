from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_pers,
    fact,
    fd_mat,
)

class MaterialWithTime(_FactCondition):
    """
    This is an extension of the Material Class, to also contain time of day-keys.
    """
    dimensions = {'MATERIAL'}
    d_table = fd_mat
    code_column = fd_mat.CONTEXT_MATERIAL_CODE
    category_column = fd_mat.MATERIAL_TYPE
    description_column = fd_mat.MATERIAL_NAME

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        fact.CAREGIVER_GROUP_KEY,
        fact.FACILITY_KEY,
        d_table.CONTEXT_NAME,
        fact.ENCOUNTER_KEY,
        description_column,
        code_column,
    ]
