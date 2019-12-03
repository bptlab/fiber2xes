from typing import Optional
from fiber.condition.fact.fact import _FactCondition
from fiber.database.table import (
    d_pers,
    d_enc,
    fact,
)

"""
This extends the Encounter Class to also have the Visit key, which allows us to filter based on visits
"""


class EncounterWithVisit(_FactCondition):
    """
    Encounters are parts of the building-blocks of FIBER. In order to define
    Cohorts, Encounters categorize patients as either inpatient or outpatient,
    meaning they stayed in hospital or were treated somewhere else, and define
    categories for this, like 'emergency'.

    The Encounter adds functionality to the FactCondition. It allows to combine
    SQL Statements that shall be performed on the FACT-Table with dimension
    'ENCOUNTER' (and optionally age-constraints on the dates).

    It also defines default-columns to return, MEDICAL_RECORD_NUMBER,
    AGE_IN_DAYS, ENCOUNTER_TYPE, ENCOUNTER_CLASS, BEGIN_DATE_AGE_IN_DAYS and
    END_DATE_AGE_IN_DAYS in this case respectively.
    """
    dimensions = {'ENCOUNTER'}
    d_table = d_enc
    code_column = d_enc.ENCOUNTER_TYPE
    category_column = d_enc.ENCOUNTER_TYPE
    description_column = d_enc.ENCOUNTER_TYPE

    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        fact.AGE_IN_DAYS,
        d_enc.ENCOUNTER_TYPE,
        d_enc.ENCOUNTER_CLASS,
        d_enc.BEGIN_DATE_AGE_IN_DAYS,
        d_enc.END_DATE_AGE_IN_DAYS,
        d_enc.ENCOUNTER_VISIT_ID,
        d_enc.ENCOUNTER_KEY
    ]

    def __init__(self, category: Optional[str] = '', **kwargs):
        """
        Args:
            category: the category of the encounter to query for
            kwargs: the keyword-arguments to pass higher in the hierarchy
        """
        if category:
            kwargs['category'] = category
        super().__init__(**kwargs)

    def _create_clause(self):
        """
        Used to create a SQLAlchemy clause based on the Encounter-condition.
        It is used to select the correct encoutners based on the category
        provided at initialization-time.
        """
        clause = super()._create_clause()
        if self._attrs['category']:
            clause &= _case_insensitive_like(
                d_enc.ENCOUNTER_TYPE, self._attrs['category'])
        return clause
