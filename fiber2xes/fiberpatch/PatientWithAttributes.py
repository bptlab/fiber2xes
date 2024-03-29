"""
Extension of the Patient class
"""
from typing import Optional

from sqlalchemy import orm

from fiber.condition import _BaseCondition, _DatabaseCondition  # type: ignore
from fiber.condition.database import _case_insensitive_like  # type: ignore
from fiber.database.table import d_pers, fact  # type: ignore


class PatientWithAttributes(_DatabaseCondition):
    """
    This is an extension of the Patient Class, to contain more attributes.
    """

    base_table = d_pers
    _default_columns = [
        d_pers.MEDICAL_RECORD_NUMBER,
        d_pers.DATE_OF_BIRTH,
        d_pers.MONTH_OF_BIRTH,
        d_pers.GENDER,
        d_pers.RELIGION,
        d_pers.RACE,
        d_pers.PATIENT_ETHNIC_GROUP,
        d_pers.DECEASED_INDICATOR,
        d_pers.MOTHER_ACCOUNT_NUMBER,
        d_pers.ADDRESS_ZIP,
        d_pers.MARITAL_STATUS_CODE,
        d_pers.LANGUAGE,
        d_pers.CITIZENSHIP,
    ]

    mrn_column = d_pers.MEDICAL_RECORD_NUMBER
    age_in_days = fact.AGE_IN_DAYS

    def __init__(
        self,
        gender: Optional[str] = None,
        religion: Optional[str] = None,
        race: Optional[str] = None,
        **kwargs
    ):
        """
        Args:
            String gender: The gender of the Patient (e.g. 'Male','Female')
            String religion:
                The religion of the Patient (e.g. 'Hindu','Catholic')
            String race: The race of the Patient (e.g. 'Hispanic/Latino')

        The string parameters are used in the SQL-LIKE statement after being
        converted to uppercase. This means that  ``%``, ``_`` and  ``[]`` can
        be used to more precisely select patients.
        """
        super().__init__(**kwargs)
        self._attrs['gender'] = gender
        self._attrs['religion'] = religion
        self._attrs['race'] = race

    def _create_clause(self):
        """
        Used to create a SQLAlchemy clause based on the Patient-condition.
        It is used to select the correct patients based on the category
        provided at initialization-time.
        """
        clause = super()._create_clause()

        if self._attrs['gender']:
            clause &= _case_insensitive_like(
                d_pers.GENDER, self._attrs['gender'])
        if self._attrs['religion']:
            clause &= _case_insensitive_like(
                d_pers.RELIGION, self._attrs['religion'])
        if self._attrs['race']:
            clause &= _case_insensitive_like(
                d_pers.RACE, self._attrs['race'])

        return clause

    def _create_query(self):
        """
        Creates an instance of a SQLAlchemy query which only returns MRNs.

        This query should yield all medical record numbers in the
        ``base_table`` of the condition. It uses the ``.clause`` to select
        the relevant patients.

        This query is also used by other functions which change the selected
        columns to get data about the patients.
        """

        return orm.Query(self.base_table).filter(
            self.clause
        ).filter(
            d_pers.ACTIVE_FLAG == 'Y'
        ).with_entities(
            self.mrn_column
        ).distinct()

    def __and__(self, other: _DatabaseCondition):
        """
        The Patient has its own __and__ function, because the SQL can be easily
        combined to optimize performance.
        """
        if (
            isinstance(other, PatientWithAttributes)
            and not (self._mrns or other._mrns)
        ):
            return self.__class__(
                dimensions=self.dimensions | other.dimensions,
                clause=self.clause & other.clause,
                children=[self, other],
                operator=_BaseCondition.AND,
            )

        return super().__and__(self, other)  # pylint: disable=too-many-function-args
