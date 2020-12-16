# coding: utf-8

"""
Модуль реализующий абстрактные классы пакета irbis.records
"""

from abc import ABCMeta
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from typing import Any, Optional


class ValueMixin:
    """
    Примесь для проверки атрибута value
    """
    @staticmethod
    def validate_value(value: 'Any') -> 'Optional[str]':
        """
        Валидация Field.value и SubField.value

        :value: значение
        """
        if value is None:
            return value
        if isinstance(value, str):
            if value:
                return value
            raise ValueError('value не может быть пустой строкой')
        raise TypeError('Не поддерживаемый тип value')


class AbstractRecord:
    """
    Родительский класс для создания альтернативных классов Record.
    """
    __metaclass__ = ABCMeta
    field_type: 'Any'
