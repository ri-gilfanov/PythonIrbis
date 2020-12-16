import asyncio
import random
from typing import TYPE_CHECKING

from irbis._common import irbis_event_loop, REGISTER_CLIENT, UNREGISTER_CLIENT

from irbis.connection.base import BaseConnection
from irbis.ini import IniFile
from irbis.query import ClientQuery
from irbis.response import ServerResponse
if TYPE_CHECKING:
    pass


class AsyncConnection(BaseConnection):

    async def connect_async(self) -> IniFile:
        """
        Асинхронное подключение к серверу ИРБИС64.

        :return: INI-файл
        """
        if self.connected:
            return self.ini_file

        while True:
            self.query_id = 0
            self.client_id = random.randint(100000, 999999)
            query = ClientQuery(self, REGISTER_CLIENT)
            query.ansi(self.username).ansi(self.password)
            response = await self.execute_async(query)
            if response.get_return_code() == -3337:
                response.close()
                continue

            result = self._connect(response)
            response.close()
            return result

    async def disconnect_async(self) -> None:
        """
        Асинхронное отключение от сервера.

        :return: None.
        """
        if self.connected:
            query = ClientQuery(self, UNREGISTER_CLIENT)
            query.ansi(self.username)
            response = await self.execute_async(query)
            response.close()
            self.connected = False

    async def execute_async(self, query: ClientQuery) -> ServerResponse:
        """
        Асинхронное исполнение запроса.
        ВНИМАНИЕ: сначала должна быть выполнена инициализация init_async()!

        :param query: Запрос.
        :return: Ответ сервера.
        """
        self.last_error = 0
        reader, writer = await asyncio.open_connection(self.host,
                                                       self.port,
                                                       loop=irbis_event_loop)
        packet = query.encode()
        writer.write(packet)
        result = ServerResponse(self)
        await result.read_data_async(reader)
        result.initial_parse()
        writer.close()
        return result
