"""
Extension of MetaData class from fiber
"""

from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_meta,
)


class MetaDataWithOnlyLevels(_FactCondition):
    """
    MetaData are parts of the building-blocks of FIBER. In order to define
    Cohorts, MetaData identifies 'conditions' that are not easily or commonly
    stored in EHR-DB's the same way.
    It is used to query for conditions that are not purely stored as diagnoses.
    The MetaData adds functionality to the FactCondition.
    It defines default-columns to return, LEVEL1, LEVEL2,
    LEVEL3, LEVEL4 and the META_DATA_KEY in this case respectively.
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