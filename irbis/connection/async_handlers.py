import random
from typing import TYPE_CHECKING

from irbis._common import GET_MAX_MFN, IRBIS_DELIMITER, irbis_to_dos,\
    NOP, READ_DOCUMENT, REGISTER_CLIENT, RESTART_SERVER, throw_value_error,\
    UNREGISTER_CLIENT

from irbis.connection.async_connection import AsyncConnection
from irbis.ini import IniFile
from irbis.query import ClientQuery
from irbis.records import Record
from irbis.specification import FileSpecification
if TYPE_CHECKING:
    from typing import Any, List, Optional, Union


class AsyncHandlers(AsyncConnection):

    async def format_record_async(self, script: str,
                                  record: 'Union[Record, int]') -> str:
        """
        Асинхронное форматирование записи с указанным MFN.

        :param script: Текст формата
        :param record: MFN записи либо сама запись
        :return: Результат расформатирования
        """
        if self.check_connection() and script:
            query = self.prepare_format_record(script, record)

            response = await self.execute_async(query)
            if not response.check_return_code():
                return ''

            result = response.utf_remaining_text().strip('\r\n')
            response.close()
            return result
        return ''

    async def get_max_mfn_async(self, database: 'Optional[str]' = None) -> int:
        """
        Асинхронное получение максимального MFN.

        :param database: База данных.
        :return: MFN, который будет присвоен следующей записи.
        """
        database = database or self.database or throw_value_error()
        assert isinstance(database, str)
        query = ClientQuery(self, GET_MAX_MFN)
        query.ansi(database)
        response = await self.execute_async(query)
        response.check_return_code()
        result = response.return_code
        response.close()
        return result

    async def nop_async(self) -> bool:
        """
        Асинхронная пустая операция.

        :return: Признак успешности операции.
        """
        if not self.check_connection():
            return False

        query = ClientQuery(self, NOP)
        response = await self.execute_async(query)
        response.close()
        return True

    async def read_record_async(self, mfn: int, version: int = 0) -> 'Optional[Record]':
        """
        Асинхронное чтение записи.

        :param mfn: MFN считываемой записи.
        :param version: версия
        :return: Прочитанная запись.
        """
        if self.check_connection():
            query = self._prepare_read_record(mfn, version)
            response = await self.execute_async(query)
            return self._complete_read_record(response, mfn, version)
        return None

    async def read_text_file_async(
        self,
        specification: 'Union[FileSpecification, str]',
    ) -> str:
        """
        Асинхронное получение содержимого текстового файла с сервера.

        :param specification: Спецификация или имя файла
            (если он находится в папке текущей базы данных).
        :return: Текст файла или пустая строка, если файл не найден
        """
        if self.check_connection():
            if isinstance(specification, str):
                specification = self.near_master(specification)
            query = ClientQuery(self, READ_DOCUMENT).ansi(str(specification))
            response = await self.execute_async(query)
            result = response.ansi_remaining_text()
            result = irbis_to_dos(result)
            response.close()
            return result
        return ''

    async def restart_server_async(self) -> bool:
        """
        Асинхронный перезапуск сервера (без утери подключенных клиентов).
        :return: Признак успешности операции.
        """
        if not self.check_connection():
            return False

        query = ClientQuery(self, RESTART_SERVER)
        response = await self.execute_async(query)
        response.close()
        return True

    async def search_async(self, parameters: 'Any') -> 'List[int]':
        """
        Асинхронный поиск записей.

        :param parameters: Параметры поиска.
        :return: Список найденных MFN.
        """
        if not self.check_connection():
            return []
        parameters, query = self._prepare_search_query(parameters)
        response = await self.execute_async(query)
        result = self._complete_search_query(parameters, response)
        response.close()
        return result

    async def search_count_async(self, expression: 'Any') -> int:
        """
        Асинхронное получение количества найденных записей.

        :param expression: Поисковый запрос.
        :return: Количество найденных записей.
        """
        if self.check_connection():
            query = self._prepare_search_count(expression)

            response = await self.execute_async(query)
            if not response.check_return_code():
                return 0

            result = response.number()
            response.close()
            return result
        return 0

    async def write_record_async(self, record: Record,
                                 lock: bool = False,
                                 actualize: bool = True,
                                 dont_parse: bool = False) -> int:
        """
        Асинхронное сохранение записи на сервере.

        :param record: Запись.
        :param lock: Оставить запись заблокированной?
        :param actualize: Актуализировать запись?
        :param dont_parse: Не разбирать ответ сервера?
        :return: Новый максимальный MFN.
        """
        if self.check_connection():
            record, database, query = self._prepare_write_record(record, Record, lock, actualize)
            response = await self.execute_async(query)
            return self._complete_write_record(record, database, response, dont_parse)
        return 0

    async def write_records_async(self, records: 'List[Record]') -> bool:
        """
        Сохранение нескольких записей на сервере.
        Записи могут принадлежать разным базам.

        # TODO: покрыть тестами

        :param records: Записи для сохранения.
        :return: Результат.
        """
        if self.check_connection():
            if not records:
                return True

            if len(records) == 1:
                return bool(await self.write_record_async(records[0]))

            query = ClientQuery(self, "6")
            query.add(0).add(1)

            for record in records:
                database = record.database or self.database
                line = database + IRBIS_DELIMITER + \
                    IRBIS_DELIMITER.join(record.encode())
                query.utf(line)

            response = await self.execute_async(query)
            response.check_return_code()
            response.close()
            return True
        return False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect_async()
        return exc_type is None
