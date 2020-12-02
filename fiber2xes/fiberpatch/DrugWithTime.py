"""
Extension of the Drug Class
"""

from typing import Optional

from fiber.database.table import fd_mat  # type: ignore

from .MaterialWithTime import MaterialWithTime


class DrugWithTime(MaterialWithTime):
    """
    This is an extension of the Drug Class, to also contain time of day-keys.
    """

    def __init__(
        self,
        name: Optional[str] = '',
        brand: Optional[str] = '',
        generic: Optional[str] = '',
        *args,
        **kwargs
    ):
        kwargs['category'] = 'Drug'
        super().__init__(*args, **kwargs)
        self._attrs['name'] = name
        self._attrs['brand'] = brand
        self._attrs['generic'] = generic

    @property
    def name(self):
        """
        Returns the name of the drug
        """
        return self._attrs['name']

    def _create_clause(self):
        clause = super()._create_clause()
        if self.name:
            clause &= (
                # type: ignore
                _multi_like_clause(fd_mat.MATERIAL_NAME, self.name) |
                # type: ignore
                _multi_like_clause(fd_mat.GENERIC_NAME, self.name) |
                _multi_like_clause(fd_mat.BRAND1, self.name) |  # type: ignore
                _multi_like_clause(fd_mat.BRAND2, self.name)  # type: ignore
            )
        if self._attrs['brand']:
            clause &= (
                # type: ignore
                _multi_like_clause(fd_mat.BRAND1, self._attrs['brand']) |
                _multi_like_clause(
                    fd_mat.BRAND2, self._attrs['brand'])  # type: ignore
            )
        if self._attrs['generic']:
            clause &= _multi_like_clause(  # type: ignore
                fd_mat.GENERIC_NAME, self._attrs['generic'])

        return clause
