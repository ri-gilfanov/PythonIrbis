import socket
import random
from typing import TYPE_CHECKING

from irbis._common import REGISTER_CLIENT, throw_value_error,\
    UNREGISTER_CLIENT

from irbis.connection.base import BaseConnection
from irbis.ini import IniFile
from irbis.query import ClientQuery
from irbis.response import ServerResponse
if TYPE_CHECKING:
    from typing import Optional


class SyncConnection(BaseConnection):
    def connect(self, host: 'Optional[str]' = None,
                port: int = 0,
                username: 'Optional[str]' = None,
                password: 'Optional[str]' = None,
                database: 'Optional[str]' = None) -> IniFile:
        """
        Подключение к серверу ИРБИС64.

        :return: INI-файл
        """
        if self.connected:
            return self.ini_file

        self.host = host or self.host or throw_value_error()
        self.port = port or self.port or int(throw_value_error())
        self.username = username or self.username or throw_value_error()
        self.password = password or self.password or throw_value_error()
        self.database = database or self.database or throw_value_error()

        assert isinstance(self.host, str)
        assert isinstance(self.port, int)
        assert isinstance(self.username, str)
        assert isinstance(self.password, str)

        while True:
            self.query_id = 0
            self.client_id = random.randint(100000, 999999)
            query = ClientQuery(self, REGISTER_CLIENT)
            query.ansi(self.username).ansi(self.password)
            with self.execute(query) as response:
                if response.get_return_code() == -3337:
                    continue

                return self._connect(response)

    def disconnect(self) -> None:
        """
        Отключение от сервера.

        :return: None.
        """
        if self.connected:
            query = ClientQuery(self, UNREGISTER_CLIENT)
            query.ansi(self.username)
            self.execute_forget(query)
            self.connected = False

    def execute(self, query: ClientQuery) -> ServerResponse:
        """
        Выполнение произвольного запроса к серверу.

        :param query: Запрос
        :return: Ответ сервера (не забыть закрыть!)
        """
        self.last_error = 0
        sock = socket.socket()
        sock.connect((self.host, self.port))
        packet = query.encode()
        sock.send(packet)
        result = ServerResponse(self)
        result.read_data(sock)
        result.initial_parse()
        return result
