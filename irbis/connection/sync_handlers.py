import random
from typing import TYPE_CHECKING

from irbis._common import GET_MAX_MFN, IRBIS_DELIMITER, irbis_to_dos, NOP,\
    REGISTER_CLIENT, RESTART_SERVER, throw_value_error, UNREGISTER_CLIENT

from irbis.connection.sync_connection import SyncConnection
from irbis.ini import IniFile
from irbis.query import ClientQuery
from irbis.records import Record
from irbis.specification import FileSpecification
if TYPE_CHECKING:
    from typing import Any, List, Optional, Union


class SyncHandlers(SyncConnection):

    def format_record(self, script: str, record: 'Union[Record, int]') -> str:
        """
        Форматирование записи с указанным MFN.

        :param script: Текст формата
        :param record: MFN записи либо сама запись
        :return: Результат расформатирования
        """
        if self.check_connection() and script:
            query = self.prepare_format_record(script, record)

            with self.execute(query) as response:
                if not response.check_return_code():
                    return ''

                result = response.utf_remaining_text().strip('\r\n')
                return result
        return ''

    def get_max_mfn(self, database: 'Optional[str]' = None) -> int:
        """
        Получение максимального MFN для указанной базы данных.

        :param database: База данных.
        :return: MFN, который будет присвоен следующей записи.
        """
        if not self.check_connection():
            database = database or self.database or throw_value_error()

            assert isinstance(database, str)

            with self.execute_ansi(GET_MAX_MFN, database) as response:
                if not response.check_return_code():
                    return 0
                result = response.return_code
                return result
        return 0

    def nop(self) -> bool:
        """
        Пустая операция (используется для периодического
        подтверждения подключения клиента).

        :return: Признак успешности операции.
        """
        if not self.check_connection():
            return False

        with self.execute_ansi(NOP):
            return True

    def read_record(self, mfn: int, version: int = 0) -> 'Optional[Record]':
        """
        Чтение записи с указанным MFN с сервера.

        :param mfn: MFN
        :param version: версия
        :return: Прочитанная запись
        """
        if self.check_connection():
            query = self._prepare_read_record(mfn, version)
            response = self.execute(query)
            return self._complete_read_record(response, mfn, version)
        return None

    def read_text_file(self, specification: 'Union[FileSpecification, str]') \
            -> str:
        """
        Получение содержимого текстового файла с сервера.

        :param specification: Спецификация или имя файла
        (если он находится в папке текущей базы данных).
        :return: Текст файла или пустая строка, если файл не найден
        """
        if self.check_connection():
            with self.read_text_stream(specification) as response:
                result = response.ansi_remaining_text()
                result = irbis_to_dos(result)
                return result
        return ''

    def restart_server(self) -> bool:
        """
        Перезапуск сервера (без утери подключенных клиентов).

        :return: Признак успешности операции.
        """
        if not self.check_connection():
            return False

        with self.execute_ansi(RESTART_SERVER):
            return True

    def search(self, parameters: 'Any') -> 'List[int]':
        """
        Поиск записей.

        :param parameters: Параметры поиска (либо поисковый запрос).
        :return: Список найденных MFN.
        """
        if not self.check_connection():
            return []
        parameters, query = self._prepare_search_query(parameters)
        response = self.execute(query)
        return self._complete_search_query(parameters, response)

    def search_count(self, expression: 'Any') -> int:
        """
        Количество найденных записей.

        :param expression: Поисковый запрос.
        :return: Количество найденных записей.
        """
        if self.check_connection():
            query = self._prepare_search_count(expression)

            response = self.execute(query)
            if not response.check_return_code():
                return 0

            return response.number()
        return 0

    def write_record(self, record: Record,
                     lock: bool = False,
                     actualize: bool = True,
                     dont_parse: bool = False) -> int:
        """
        Сохранение записи на сервере.

        :param record: Запись.
        :param lock: Оставить запись заблокированной?
        :param actualize: Актуализировать запись?
        :param dont_parse: Не разбирать ответ сервера?
        :return: Новый максимальный MFN.
        """
        if self.check_connection():
            record, database, query = self._prepare_write_record(record, Record, lock, actualize)
            response = self.execute(query)
            return self._complete_write_record(record, database, response, dont_parse)
        return 0

    def write_records(self, records: 'List[Record]') -> bool:
        """
        Сохранение нескольких записей на сервере.
        Записи могут принадлежать разным базам.

        :param records: Записи для сохранения.
        :return: Результат.
        """
        if self.check_connection():
            if not records:
                return True

            if len(records) == 1:
                return bool(self.write_record(records[0]))

            query = ClientQuery(self, "6")
            query.add(0).add(1)

            for record in records:
                database = record.database or self.database
                line = database + IRBIS_DELIMITER + \
                    IRBIS_DELIMITER.join(record.encode())
                query.utf(line)

            response = self.execute(query)
            response.check_return_code()
            response.close()
            return True
        return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return exc_type is None
