from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_meta,
)


class MetaDataWithOnlyLevels(_FactCondition):
    """
    MetaData are parts of the building-blocks of FIBER. In order to define
    Cohorts, MetaData identifies 'conditions' that are not easily or commonly
    stored in EHR-DB's the same way.
    It is used to query for conditions that are not purely stored as diagnoses,
    like alcohol- or tobacco-use in the respective subclasses.
    The MetaData adds functionality to the FactCondition. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'METADATA' (and optionally age-constraints on the dates).
    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, LEVEL3, LEVEL4 and the VALUE in this case respectively.
    """
    dimensions = {'METADATA'}
    d_table = d_meta
    code_column = d_meta.META_DATA_KEY
    category_column = d_meta.CONTEXT_KEY
    description_column = d_meta.META_DATA_KEY

    _default_columns = [
        d_meta.LEVEL1,
        d_meta.LEVEL2,
        d_meta.LEVEL3,
        d_meta.LEVEL4,
        d_meta.META_DATA_KEY,
    ]