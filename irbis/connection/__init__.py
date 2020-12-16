# coding: utf-8

"""
Подключение к серверу ИРБИС64.
"""

from irbis._common import NOT_CONNECTED
from irbis.connection.async_connection import AsyncConnection
from irbis.connection.async_handlers import AsyncHandlers
from irbis.connection.deprecated import DeprecatedConnMixin
from irbis.connection.sync_connection import SyncConnection
from irbis.connection.sync_handlers import SyncHandlers


class Connection(AsyncHandlers, DeprecatedConnMixin):
    """
    Подключение к серверу
    """


__all__ = ['AsyncConnection', 'AsyncHandlers', 'Connection', 'SyncConnection',
           'SyncHandlers', 'NOT_CONNECTED']
