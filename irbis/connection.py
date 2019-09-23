# coding: utf-8

"""
Подключение к серверу ИРБИС64.
"""

import asyncio
import socket
from typing import Any, List, Optional, Tuple, Union

from ._common import ACTUALIZE_RECORD, ALL, CREATE_DATABASE, \
    CREATE_DICTIONARY, DATA, DELETE_DATABASE, EMPTY_DATABASE, FORMAT_RECORD, \
    GET_MAX_MFN, GET_PROCESS_LIST, GET_USER_LIST, IRBIS_DELIMITER, \
    irbis_to_dos, \
    irbis_to_lines, irbis_event_loop, LIST_FILES, LOGICALLY_DELETED, \
    MASTER_FILE, MAX_POSTINGS, NOP, ObjectWithError, OTHER_DELIMITER, \
    READ_RECORD, \
    READ_RECORD_CODES, READ_DOCUMENT, READ_POSTINGS, READ_TERMS, \
    READ_TERMS_REVERSE, READ_TERMS_CODES, RECORD_LIST, REGISTER_CLIENT, \
    RELOAD_DICTIONARY, RELOAD_MASTER_FILE, RESTART_SERVER, safe_str, SEARCH, \
    SERVER_INFO, SET_USER_LIST, short_irbis_to_lines, SYSTEM, \
    throw_value_error, UNREGISTER_CLIENT, UNLOCK_DATABASE, UNLOCK_RECORDS, \
    UPDATE_INI_FILE, UPDATE_RECORD

from .alphabet import AlphabetTable, UpperCaseTable
from .database import DatabaseInfo
from .error import IrbisError, IrbisFileNotFoundError
from .ini import IniFile
from .menus import MenuFile
from .opt import OptFile
from .par import ParFile
from .process import Process
from .query import ClientQuery
from .record import RawRecord, Record
from .response import ServerResponse
from .search import SearchParameters, SearchScenario
from .specification import FileSpecification
from .terms import PostingParameters, TermInfo, TermPosting, TermParameters
from .tree import TreeFile
from .version import ServerVersion
from .user import UserInfo


class Connection(ObjectWithError):
    """
    Подключение к серверу
    """

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 6666
    DEFAULT_DATABASE = 'IBIS'

    __slots__ = ('host', 'port', 'username', 'password', 'database',
                 'workstation', 'client_id', 'query_id', 'connected',
                 '_stack', 'server_version', 'ini_file')

    def __init__(self, host: Optional[str] = None,
                 port: int = 0,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 database: Optional[str] = None,
                 workstation: str = 'C') -> None:
        super().__init__()
        self.host: str = host or Connection.DEFAULT_HOST
        self.port: int = port or Connection.DEFAULT_PORT
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.database: str = database or Connection.DEFAULT_DATABASE
        self.workstation: str = workstation
        self.client_id: int = 0
        self.query_id: int = 0
        self.connected: bool = False
        self._stack: List[str] = []
        self.server_version: Optional[str] = None
        self.ini_file: IniFile = IniFile()
        self.last_error = 0

    def actualize_record(self, mfn: int,
                         database: Optional[str] = None) -> None:
        """
        Актуализация записи с указанным MFN.

        :param mfn: MFN записи
        :param database: База данных
        :return: None
        """

        database = database or self.database or throw_value_error()

        assert isinstance(mfn, int)
        assert isinstance(database, str)

        query = ClientQuery(self, ACTUALIZE_RECORD).ansi(database).add(mfn)
        with self.execute(query) as response:
            response.check_return_code()

    def _connect(self, response: ServerResponse) -> IniFile:
        self.server_version = response.version
        result = IniFile()
        text = irbis_to_lines(response.ansi_remaining_text())
        line = text[0]
        text = line.splitlines()
        text = text[1:]
        result.parse(text)
        self.connected = True
        self.ini_file = result
        return result

    def connect(self, host: Optional[str] = None,
                port: int = 0,
                username: Optional[str] = None,
                password: Optional[str] = None,
                database: Optional[str] = None) -> IniFile:
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

        import random

        while True:
            self.query_id = 0
            self.client_id = random.randint(100000, 999999)
            query = ClientQuery(self, REGISTER_CLIENT)
            query.ansi(self.username).ansi(self.password)
            with self.execute(query) as response:
                if response.get_return_code() == -3337:
                    continue

                return self._connect(response)

    async def connect_async(self) -> IniFile:
        """
        Асинхронное подключение к серверу ИРБИС64.

        :return: INI-файл
        """
        import random

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

    def create_database(self, database: Optional[str] = None,
                        description: Optional[str] = None,
                        reader_access: bool = True) -> None:
        """
        Создание базы данных.

        :param database: Имя создаваемой базы
        :param description: Описание в свободной форме
        :param reader_access: Читатель будет иметь доступ?
        :return: None
        """

        database = database or self.database or throw_value_error()
        description = description or ''

        assert isinstance(database, str)
        assert isinstance(description, str)

        query = ClientQuery(self, CREATE_DATABASE)
        query.ansi(database).ansi(description).add(int(reader_access))
        with self.execute(query) as response:
            response.check_return_code()

    def create_dictionary(self, database: Optional[str] = None) -> None:
        """
        Создание словаря в базе данных.

        :param database: База данных
        :return: None
        """

        database = database or self.database or throw_value_error()

        assert isinstance(database, str)

        query = ClientQuery(self, CREATE_DICTIONARY).ansi(database)
        with self.execute(query) as response:
            response.check_return_code()

    def delete_database(self, database: Optional[str] = None) -> None:
        """
        Удаление базы данных.

        :param database: Имя удаляемой базы
        :return: None
        """

        assert isinstance(database, str)

        database = database or self.database or throw_value_error()
        query = ClientQuery(self, DELETE_DATABASE).ansi(database)
        with self.execute(query) as response:
            response.check_return_code()

    def delete_record(self, mfn: int) -> None:
        """
        Удаление записи по ее MFN.

        :param mfn: MFN удаляемой записи
        :return: None
        """

        assert mfn
        assert isinstance(mfn, int)

        record = self.read_record(mfn)
        if not record.is_deleted():
            record.status |= LOGICALLY_DELETED
            self.write_record(record, dont_parse=True)

    def disconnect(self) -> None:
        """
        Отключение от сервера.

        :return: None.
        """

        if not self.connected:
            return

        query = ClientQuery(self, UNREGISTER_CLIENT)
        query.ansi(self.username)
        self.execute_forget(query)
        self.connected = False

    async def disconnect_async(self) -> None:
        """
        Асинхронное отключение от сервера.

        :return: None.
        """
        if not self.connected:
            return

        query = ClientQuery(self, UNREGISTER_CLIENT)
        query.ansi(self.username)
        response = await self.execute_async(query)
        response.close()
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

    def execute_ansi(self, *commands) -> ServerResponse:
        """
        Простой запрос к серверу, когда все строки запроса
        в кодировке ANSI.

        :param commands: Команда и параметры запроса
        :return: Ответ сервера (не забыть закрыть!)
        """
        query = ClientQuery(self, commands[0])
        for line in commands[1:]:
            query.ansi(line)
        return self.execute(query)

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

    def execute_forget(self, query: ClientQuery) -> None:
        """
        Выполнение запроса к серверу, когда нам не важен результат
        (мы не собираемся его парсить).

        :param query: Клиентский запрос
        :return: None
        """
        with self.execute(query):
            pass

    # noinspection DuplicatedCode
    def format_record(self, script: str, record: Union[Record, int]) -> str:
        """
        Форматирование записи с указанным MFN.

        :param script: Текст формата
        :param record: MFN записи либо сама запись
        :return: Результат расформатирования
        """
        script = script or throw_value_error()
        if not record:
            raise ValueError()

        assert isinstance(script, str)
        assert isinstance(record, (Record, int))

        if not script:
            return ''

        query = ClientQuery(self, FORMAT_RECORD).ansi(self.database)

        query.format(script)

        if isinstance(record, int):
            query.add(1).add(record)
        else:
            query.add(-2).utf(IRBIS_DELIMITER.join(record.encode()))

        with self.execute(query) as response:
            response.check_return_code()
            result = response.utf_remaining_text().strip('\r\n')
            return result

    # noinspection DuplicatedCode
    async def format_record_async(self, script: str,
                                  record: Union[Record, int]) -> str:
        """
        Асинхронное форматирование записи с указанным MFN.

        :param script: Текст формата
        :param record: MFN записи либо сама запись
        :return: Результат расформатирования
        """
        script = script or throw_value_error()
        if not record:
            raise ValueError()

        assert isinstance(script, str)
        assert isinstance(record, (Record, int))

        if not script:
            return ''

        query = ClientQuery(self, FORMAT_RECORD).ansi(self.database)
        query.format(script)
        if isinstance(record, int):
            query.add(1).add(record)
        else:
            query.add(-2).utf(IRBIS_DELIMITER.join(record.encode()))

        response = await self.execute_async(query)
        response.check_return_code()
        result = response.utf_remaining_text().strip('\r\n')
        response.close()
        return result

    def format_records(self, script: str, records: List[int]) -> List[str]:
        """
        Форматирование группы записей по MFN.

        :param script: Текст формата
        :param records: Список MFN
        :return: Список строк
        """
        if not records:
            return []

        script = script or throw_value_error()

        assert isinstance(script, str)
        assert isinstance(records, list)

        if not script:
            return [''] * len(records)

        if len(records) > MAX_POSTINGS:
            raise IrbisError()

        query = ClientQuery(self, FORMAT_RECORD).ansi(self.database)
        query.format(script)
        query.add(len(records))
        for mfn in records:
            query.add(mfn)

        with self.execute(query) as response:
            response.check_return_code()
            result = response.utf_remaining_lines()
            result = [line.split('#', 1)[1] for line in result]
            return result

    def get_database_info(self, database: Optional[str] = None) \
            -> DatabaseInfo:
        """
        Получение информации о базе данных.

        :param database: Имя базы
        :return: Информация о базе
        """
        database = database or self.database or throw_value_error()
        query = ClientQuery(self, RECORD_LIST).ansi(database)
        with self.execute(query) as response:
            response.check_return_code()
            result = DatabaseInfo()
            result.parse(response)
            result.name = database
            return result

    def get_max_mfn(self, database: Optional[str] = None) -> int:
        """
        Получение максимального MFN для указанной базы данных.

        :param database: База данных.
        :return: MFN, который будет присвоен следующей записи.
        """

        database = database or self.database or throw_value_error()

        assert isinstance(database, str)

        with self.execute_ansi(GET_MAX_MFN, database) as response:
            response.check_return_code()
            result = response.return_code
            return result

    async def get_max_mfn_async(self, database: Optional[str] = None) -> int:
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

    def get_server_version(self) -> ServerVersion:
        """
        Получение версии сервера.

        :return: Версия сервера
        """
        query = ClientQuery(self, SERVER_INFO)
        with self.execute(query) as response:
            response.check_return_code()
            lines = response.ansi_remaining_lines()
            result = ServerVersion()
            result.parse(lines)
            if not self.server_version:
                self.server_version = result.version
            return result

    def list_databases(self, specification: str) \
            -> List[DatabaseInfo]:
        """
        Получение списка баз данных.

        :param specification: Спецификация файла, например, '1..dbnam2.mnu'
        :return: Список баз данных
        """
        menu = self.read_menu(specification)
        result: List[DatabaseInfo] = []
        for entry in menu.entries:
            db_info: DatabaseInfo = DatabaseInfo()
            db_info.name = entry.code
            if db_info.name[0] == '-':
                db_info.name = db_info.name[1:]
                db_info.read_only = True
            db_info.description = entry.comment
            result.append(db_info)

        return result

    def list_files(self,
                   *specification: Union[FileSpecification, str]) -> List[str]:
        """
        Получение списка файлов с сервера.

        :param specification: Спецификация или маска имени файла
        (если нужны файлы, лежащие в папке текущей базы данных)
        :return: Список файлов
        """
        query = ClientQuery(self, LIST_FILES)

        is_ok = False
        for spec in specification:
            if isinstance(spec, str):
                spec = self.near_master(spec)
            query.ansi(str(spec))
            is_ok = True

        result: List[str] = []
        if not is_ok:
            return result

        with self.execute(query) as response:
            lines = response.ansi_remaining_lines()
            lines = [line for line in lines if line]
            for line in lines:
                result.extend(one for one in irbis_to_lines(line) if one)
        return result

    # noinspection DuplicatedCode
    def list_processes(self) -> List[Process]:
        """
        Получение списка серверных процессов.

        :return: Список процессов
        """
        query = ClientQuery(self, GET_PROCESS_LIST)
        with self.execute(query) as response:
            response.check_return_code()
            result: List[Process] = []
            process_count = response.number()
            lines_per_process = response.number()

            if not process_count or not lines_per_process:
                return result

            for _ in range(process_count):
                process = Process()
                process.number = response.ansi()
                process.ip_address = response.ansi()
                process.name = response.ansi()
                process.client_id = response.ansi()
                process.workstation = response.ansi()
                process.started = response.ansi()
                process.last_command = response.ansi()
                process.command_number = response.ansi()
                process.process_id = response.ansi()
                process.state = response.ansi()
                result.append(process)

            return result

    def list_users(self) -> List[UserInfo]:
        """
        Получение списка пользователей с сервера.

        :return: Список пользователей
        """
        query = ClientQuery(self, GET_USER_LIST)
        with self.execute(query) as response:
            response.check_return_code()
            result = UserInfo.parse(response)
            return result

    def near_master(self, filename: str) -> FileSpecification:
        """
        Файл рядом с мастер-файлом текущей базы данных.

        :param filename: Имя файла
        :return: Спецификация файла
        """

        return FileSpecification(MASTER_FILE, self.database, filename)

    def nop(self) -> None:
        """
        Пустая операция (используется для периодического
        подтверждения подключения клиента).

        :return: None
        """
        with self.execute_ansi(NOP):
            pass

    async def nop_async(self) -> None:
        """
        Асинхронная пустая операция.

        :return: None.
        """
        query = ClientQuery(self, NOP)
        response = await self.execute_async(query)
        response.close()

    def parse_connection_string(self, text: str) -> None:
        """
        Разбор строки подключения.

        :param text: Строка подключения
        :return: None
        """

        for item in text.split(';'):
            if not item:
                continue
            parts = item.split('=', 1)
            name = parts[0].strip().lower()
            value = parts[1].strip()

            if name in ['host', 'server', 'address']:
                self.host = value

            if name == 'port':
                self.port = int(value)

            if name in ['user', 'username', 'name', 'login']:
                self.username = value

            if name in ['pwd', 'password']:
                self.password = value

            if name in ['db', 'database', 'catalog']:
                self.database = value

            if name in ['arm', 'workstation']:
                self.workstation = value

    def pop_database(self) -> str:
        """
        Восстановление подключения к прошлой базе данных,
        запомненной с помощью push_database.

        :return: Прошлая база данных
        """
        result = self.database
        self.database = self._stack.pop()
        return result

    def push_database(self, database: str) -> str:
        """
        Установка подключения к новой базе данных,
        с запоминанием предыдущей базы.

        :param database: Новая база данных
        :return: Предыдущая база данных
        """

        assert database and isinstance(database, str)

        result = self.database
        self._stack.append(result)
        self.database = database
        return result

    def read_alphabet_table(self,
                            specification: Optional[FileSpecification] =
                            None) \
            -> AlphabetTable:
        """
        Чтение алфавитной таблицы с сервера.

        :param specification: Спецификация
        :return: Таблица
        """
        if specification is None:
            specification = FileSpecification(SYSTEM, None,
                                              AlphabetTable.FILENAME)

        with self.read_text_stream(specification) as response:
            text = response.ansi_remaining_text()
            if text:
                result = AlphabetTable()
                result.parse(text)
            else:
                result = AlphabetTable.get_default()
            return result

    def read_binary_file(self, specification: Union[FileSpecification, str]) \
            -> Optional[bytearray]:
        """
        Чтение двоичного файла с сервера.

        :param specification: Спецификация
        :return: Массив байт или None
        """
        if isinstance(specification, str):
            specification = self.near_master(specification)

        assert isinstance(specification, FileSpecification)

        specification.binary = True
        query = ClientQuery(self, READ_DOCUMENT).ansi(str(specification))
        with self.execute(query) as response:
            result = response.get_binary_file()
            return result

    def read_ini_file(self, specification: Union[FileSpecification, str]) \
            -> IniFile:
        """
        Чтение INI-файла с сервера.

        :param specification: Спецификация
        :return: INI-файл
        """
        if isinstance(specification, str):
            specification = self.near_master(specification)

        assert isinstance(specification, FileSpecification)

        query = ClientQuery(self, READ_DOCUMENT).ansi(str(specification))
        with self.execute(query) as response:
            result = IniFile()
            text = irbis_to_lines(response.ansi_remaining_text())
            result.parse(text)
            return result

    def read_menu(self, specification: Union[FileSpecification, str]) \
            -> MenuFile:
        """
        Чтение меню с сервера.

        :param specification: Спецификация файла
        :return: Меню
        """

        with self.read_text_stream(specification) as response:
            result = MenuFile()
            text = irbis_to_lines(response.ansi_remaining_text())
            result.parse(text)
            return result

    def read_opt_file(self, specification: Union[FileSpecification, str]) \
            -> OptFile:
        """
        Получение файла оптимизации рабочих листов с сервера.

        :param specification: Спецификация
        :return: Файл оптимизации
        """
        with self.read_text_stream(specification) as response:
            result = OptFile()
            text = irbis_to_lines(response.ansi_remaining_text())
            result.parse(text)
            return result

    def read_par_file(self, specification: Union[FileSpecification, str]) \
            -> ParFile:
        """
        Получение PAR-файла с сервера.

        :param specification: Спецификация или имя файла (если он в папке DATA)
        :return: Полученный файл
        """
        if isinstance(specification, str):
            specification = FileSpecification(DATA, None, specification)

        with self.read_text_stream(specification) as response:
            result = ParFile()
            text = irbis_to_lines(response.ansi_remaining_text())
            result.parse(text)
            return result

    def read_postings(self, parameters: Union[PostingParameters, str],
                      fmt: Optional[str] = None) -> List[TermPosting]:
        """
        Считывание постингов для указанных термов из поискового словаря.

        :param parameters: Параметры постингов или терм
        :param fmt: Опциональный формат
        :return: Список постингов
        """
        if isinstance(parameters, str):
            parameters = PostingParameters(parameters)
            parameters.fmt = fmt

        database = parameters.database or self.database or throw_value_error()
        query = ClientQuery(self, READ_POSTINGS)
        query.ansi(database).add(parameters.number)
        query.add(parameters.first).ansi(parameters.fmt)
        for term in parameters.terms:
            query.utf(term)
        with self.execute(query) as response:
            response.check_return_code(READ_TERMS_CODES)
            result = []
            while True:
                line = response.utf()
                if not line:
                    break
                posting = TermPosting()
                posting.parse(line)
                result.append(posting)
            return result

    def read_raw_record(self, mfn: int) -> RawRecord:
        """
        Чтение сырой записи с сервера.

        :param mfn: MFN записи.
        :return: Загруженная с сервера запись.
        """
        mfn = mfn or int(throw_value_error())

        query = ClientQuery(self, READ_RECORD)
        query.ansi(self.database).add(mfn)
        with self.execute(query) as response:
            response.check_return_code(READ_RECORD_CODES)
            text = response.utf_remaining_lines()
            result = RawRecord()
            result.database = self.database
            result.parse(text)

        return result

    def read_record(self, mfn: int, version: int = 0) -> Record:
        """
        Чтение записи с указанным MFN с сервера.

        :param mfn: MFN
        :param version: версия
        :return: Прочитанная запись
        """

        mfn = mfn or int(throw_value_error())

        assert isinstance(mfn, int)

        query = ClientQuery(self, READ_RECORD).ansi(self.database).add(mfn)
        if version:
            query.add(version)
        with self.execute(query) as response:
            response.check_return_code(READ_RECORD_CODES)
            text = response.utf_remaining_lines()
            result = Record()
            result.database = self.database
            result.parse(text)

        if version:
            self.unlock_records([mfn])

        return result

    async def read_record_async(self, mfn: int) -> Record:
        """
        Асинхронное чтение записи.

        :param mfn: MFN считываемой записи.
        :return: Прочитанная запись.
        """
        mfn = mfn or int(throw_value_error())
        assert isinstance(mfn, int)
        query = ClientQuery(self, READ_RECORD).ansi(self.database).add(mfn)
        response = await self.execute_async(query)
        response.check_return_code(READ_RECORD_CODES)
        text = response.utf_remaining_lines()
        result = Record()
        result.database = self.database
        result.parse(text)
        response.close()
        return result

    def read_record_postings(self, mfn: int, prefix: str) -> List[TermPosting]:
        """
        Получение постингов для указанных записи и префикса.

        :param mfn: MFN записи.
        :param prefix: Префикс в виде "A=$".
        :return: Список постингов.
        """
        assert mfn > 0

        query = ClientQuery(self, 'V')
        query.ansi(self.database).add(mfn).utf(prefix)
        result: List[TermPosting] = []
        with self.execute(query) as response:
            response.check_return_code()
            lines = response.utf_remaining_lines()
            for line in lines:
                one: TermPosting = TermPosting()
                one.parse(line)
                result.append(one)
        return result

    def read_records(self, *mfns: int) -> List[Record]:
        """
        Чтение записей с указанными MFN с сервера.

        :param mfns: Перечень MFN
        :return: Список записей
        """

        array = list(mfns)

        if not array:
            return []

        if len(array) == 1:
            return [self.read_record(array[0])]

        lines = self.format_records(ALL, array)
        result = []
        for line in lines:
            parts = line.split(OTHER_DELIMITER)
            if parts:
                parts = [x for x in parts[1:] if x]
                record = Record()
                record.parse(parts)
                if record:
                    record.database = self.database
                    result.append(record)

        return result

    def read_search_scenario(self,
                             specification: Union[FileSpecification, str]) \
            -> List[SearchScenario]:
        """
        Read search scenario from the server.

        :param specification: File which contains the scenario
        :return: List of the scenarios (possibly empty)
        """
        if isinstance(specification, str):
            specification = self.near_master(specification)

        with self.read_text_stream(specification) as response:
            ini = IniFile()
            text = irbis_to_lines(response.ansi_remaining_text())
            ini.parse(text)
            result = SearchScenario.parse(ini)
            return result

    def read_terms(self,
                   parameters: Union[TermParameters, str, Tuple[str, int]]) \
            -> List[TermInfo]:
        """
        Получение термов поискового словаря.

        :param parameters: Параметры термов или терм
            или кортеж "терм, количество"
        :return: Список термов
        """
        if isinstance(parameters, tuple):
            parameters2 = TermParameters(parameters[0])
            parameters2.number = parameters[1]
            parameters = parameters2

        if isinstance(parameters, str):
            parameters = TermParameters(parameters)
            parameters.number = 10

        assert isinstance(parameters, TermParameters)

        database = parameters.database or self.database or throw_value_error()
        command = READ_TERMS_REVERSE if parameters.reverse else READ_TERMS
        query = ClientQuery(self, command)
        query.ansi(database).utf(parameters.start)
        query.add(parameters.number).ansi(parameters.format)
        with self.execute(query) as response:
            response.check_return_code(READ_TERMS_CODES)
            lines = response.utf_remaining_lines()
            result = TermInfo.parse(lines)
            return result

    def read_text_file(self, specification: Union[FileSpecification, str]) \
            -> str:
        """
        Получение содержимого текстового файла с сервера.

        :param specification: Спецификация или имя файла
        (если он находится в папке текущей базы данных).
        :return: Текст файла или пустая строка, если файл не найден
        """

        with self.read_text_stream(specification) as response:
            result = response.ansi_remaining_text()
            result = irbis_to_dos(result)
            return result

    async def read_text_file_async(self,
                                   specification: Union[FileSpecification,
                                                        str]) -> str:
        """
        Асинхронное получение содержимого текстового файла с сервера.

        :param specification: Спецификация или имя файла
            (если он находится в папке текущей базы данных).
        :return: Текст файла или пустая строка, если файл не найден
        """
        if isinstance(specification, str):
            specification = self.near_master(specification)
        query = ClientQuery(self, READ_DOCUMENT).ansi(str(specification))
        response = await self.execute_async(query)
        result = response.ansi_remaining_text()
        result = irbis_to_dos(result)
        response.close()
        return result

    def read_text_stream(self, specification: Union[FileSpecification, str]) \
            -> ServerResponse:
        """
        Получение текстового файла с сервера в виде потока.

        :param specification: Спецификация или имя файла
        (если он находится в папке текущей базы данных).
        :return: ServerResponse, из которого можно считывать строки
        """

        if isinstance(specification, str):
            specification = self.near_master(specification)

        assert isinstance(specification, FileSpecification)

        query = ClientQuery(self, READ_DOCUMENT).ansi(str(specification))
        result = self.execute(query)
        return result

    def read_tree_file(self, specification: Union[FileSpecification, str]) \
            -> TreeFile:
        """
        Чтение TRE-файла с сервера.

        :param specification:  Спецификация
        :return: Дерево
        """
        with self.read_text_stream(specification) as response:
            text = response.ansi_remaining_text()
            text = [line for line in irbis_to_lines(text) if line]
            result = TreeFile()
            result.parse(text)
            return result

    def read_uppercase_table(self,
                             specification: Optional[FileSpecification] =
                             None) \
            -> UpperCaseTable:
        """
        Чтение таблицы преобразования в верхний регистр с сервера.

        :param specification: Спецификация
        :return: Таблица
        """
        if specification is None:
            specification = FileSpecification(SYSTEM,
                                              None,
                                              UpperCaseTable.FILENAME)

        with self.read_text_stream(specification) as response:
            text = response.ansi_remaining_text()
            if text:
                result = UpperCaseTable()
                result.parse(text)
            else:
                result = UpperCaseTable.get_default()
            return result

    def reload_dictionary(self, database: Optional[str] = None) -> None:
        """
        Пересоздание словаря.

        :param database: База данных
        :return: None
        """

        database = database or self.database or throw_value_error()

        assert isinstance(database, str)

        with self.execute_ansi(RELOAD_DICTIONARY, database):
            pass

    def reload_master_file(self, database: Optional[str] = None) -> None:
        """
        Пересоздание мастер-файла.

        :param database: База данных
        :return: None
        """

        database = database or self.database or throw_value_error()

        assert isinstance(database, str)

        with self.execute_ansi(RELOAD_MASTER_FILE, database):
            pass

    def restart_server(self) -> None:
        """
        Перезапуск сервера (без утери подключенных клиентов).

        :return: None
        """

        with self.execute_ansi(RESTART_SERVER):
            pass

    def require_alphabet_table(self,
                               specification: Optional[FileSpecification] =
                               None) \
            -> AlphabetTable:
        """
        Чтение алфавитной таблицы с сервера.
        :param specification: Спецификация
        :return: Таблица
        """
        if specification is None:
            specification = FileSpecification(SYSTEM,
                                              None,
                                              AlphabetTable.FILENAME)

        with self.read_text_stream(specification) as response:
            text = response.ansi_remaining_text()
            if not text:
                raise IrbisFileNotFoundError(specification)
            if text:
                result = AlphabetTable()
                result.parse(text)
            else:
                result = AlphabetTable.get_default()
            return result

    def require_menu(self,
                     specification: Union[FileSpecification, str]) -> MenuFile:
        """
        Чтение меню с сервера.

        :param specification: Спецификация файла
        :return: Меню
        """
        with self.read_text_stream(specification) as response:
            result = MenuFile()
            text = irbis_to_lines(response.ansi_remaining_text())
            if not text:
                raise IrbisFileNotFoundError(specification)
            result.parse(text)
            return result

    def require_opt_file(self,
                         specification: Union[FileSpecification, str]) \
            -> OptFile:
        """
        Получение файла оптимизации рабочих листов с сервера.

        :param specification: Спецификация
        :return: Файл оптимизации
        """
        with self.read_text_stream(specification) as response:
            result = OptFile()
            text = irbis_to_lines(response.ansi_remaining_text())
            if not text:
                raise IrbisFileNotFoundError(specification)
            result.parse(text)
            return result

    def require_par_file(self,
                         specification: Union[FileSpecification, str]) \
            -> ParFile:
        """
        Получение PAR-файла с сервера.

        :param specification: Спецификация или имя файла (если он в папке DATA)
        :return: Полученный файл
        """
        if isinstance(specification, str):
            specification = FileSpecification(DATA, None, specification)

        with self.read_text_stream(specification) as response:
            result = ParFile()
            text = irbis_to_lines(response.ansi_remaining_text())
            if not text:
                raise IrbisFileNotFoundError(specification)
            result.parse(text)
            return result

    def require_text_file(self,
                          specification: FileSpecification) -> str:
        """
        Чтение текстового файла с сервера.

        :param specification: Спецификация
        :return: Содержимое файла
        """
        result = self.read_text_file(specification)
        if not result:
            raise IrbisFileNotFoundError(specification)

        return result

    def search(self, parameters: Any) -> List[int]:
        """
        Поиск записей.

        :param parameters: Параметры поиска (либо поисковый запрос)
        :return: Список найденных MFN
        """
        if not isinstance(parameters, SearchParameters):
            parameters = SearchParameters(str(parameters))

        database = parameters.database or self.database or throw_value_error()
        query = ClientQuery(self, SEARCH)
        query.ansi(database)
        query.utf(parameters.expression)
        query.add(parameters.number)
        query.add(parameters.first)
        query.ansi(parameters.format)
        query.add(parameters.min_mfn)
        query.add(parameters.max_mfn)
        query.ansi(parameters.sequential)
        response = self.execute(query)
        response.check_return_code()
        _ = response.number()  # Число найденных записей
        result = []
        while 1:
            line = response.ansi()
            if not line:
                break
            mfn = int(line)
            result.append(mfn)
        return result

    def search_count(self, expression: Any) -> int:
        """
        Количество найденных записей.

        :param expression: Поисковый запрос.
        :return: Количество найденных записей.
        """
        expression = str(expression)

        query = ClientQuery(self, SEARCH)
        query.ansi(self.database)
        query.utf(expression)
        query.add(0)
        query.add(0)
        response = self.execute(query)
        response.check_return_code()
        return response.number()

    def to_connection_string(self) -> str:
        """
        Выдача строки подключения для текущего соединения.

        :return: Строка подключения
        """

        return 'host=' + safe_str(self.host) + \
               ';port=' + str(self.port) + \
               ';username=' + safe_str(self.username) + \
               ';password=' + safe_str(self.password) + \
               ';database=' + safe_str(self.database) + \
               ';workstation=' + safe_str(self.workstation) + ';'

    def truncate_database(self, database: Optional[str] = None) -> None:
        """
        Опустошение базы данных.

        :param database: База данных
        :return: None
        """

        database = database or self.database or throw_value_error()

        assert isinstance(database, str)

        with self.execute_ansi(EMPTY_DATABASE, database):
            pass

    def undelete_record(self, mfn: int) -> None:
        """
        Восстановление записи по ее MFN.

        :param mfn: MFN восстанавливаемой записи
        :return: None
        """

        assert mfn
        assert isinstance(mfn, int)

        record = self.read_record(mfn)
        if record.is_deleted():
            record.status &= ~LOGICALLY_DELETED
            self.write_record(record, dont_parse=True)

    def unlock_database(self, database: Optional[str] = None) -> None:
        """
        Разблокирование базы данных.

        :param database: Имя базы
        :return: None
        """

        database = database or self.database or throw_value_error()

        assert isinstance(database, str)

        with self.execute_ansi(UNLOCK_DATABASE, database):
            pass

    def unlock_records(self, records: List[int],
                       database: Optional[str] = None) -> None:
        """
        Разблокирование записей.

        :param records: Список MFN
        :param database: База данных
        :return: None
        """

        if not records:
            return

        database = database or self.database or throw_value_error()

        assert isinstance(database, str)

        query = ClientQuery(self, UNLOCK_RECORDS).ansi(database)
        for mfn in records:
            query.add(mfn)
        with self.execute(query) as response:
            response.check_return_code()

    def update_ini_file(self, lines: List[str]) -> None:
        """
        Обновление строк серверного INI-файла.

        :param lines: Измененные строки
        :return: None
        """
        if not lines:
            return

        query = ClientQuery(self, UPDATE_INI_FILE)
        for line in lines:
            query.ansi(line)
        self.execute_forget(query)

    def update_user_list(self, users: List[UserInfo]) -> None:
        """
        Обновление списка пользователей на сервере.

        :param users:  Список пользователей
        :return: None
        """
        assert isinstance(users, list) and users

        query = ClientQuery(self, SET_USER_LIST)
        for user in users:
            query.ansi(user.encode())
        self.execute_forget(query)

    # noinspection DuplicatedCode
    def write_raw_record(self, record: RawRecord,
                         lock: bool = False,
                         actualize: bool = True) -> int:
        """
        Сохранение записи на сервере.

        :param record: Запись
        :param lock: Оставить запись заблокированной?
        :param actualize: Актуализировать запись?
        :return: Новый максимальный MFN
        """
        database = record.database or self.database or throw_value_error()
        if not record:
            raise ValueError()

        assert isinstance(record, RawRecord)
        assert isinstance(database, str)

        assert isinstance(record, RawRecord)
        assert isinstance(database, str)

        query = ClientQuery(self, UPDATE_RECORD)
        query.ansi(database).add(int(lock)).add(int(actualize))
        query.utf(IRBIS_DELIMITER.join(record.encode()))
        with self.execute(query) as response:
            response.check_return_code()
            result = response.return_code  # Новый максимальный MFN
            return result

    # noinspection DuplicatedCode
    def write_record(self, record: Record,
                     lock: bool = False,
                     actualize: bool = True,
                     dont_parse: bool = False) -> int:
        """
        Сохранение записи на сервере.

        :param record: Запись
        :param lock: Оставить запись заблокированной?
        :param actualize: Актуализировать запись?
        :param dont_parse: Не разбирать ответ сервера?
        :return: Новый максимальный MFN
        """

        database = record.database or self.database or throw_value_error()
        if not record:
            raise ValueError()

        assert isinstance(record, Record)
        assert isinstance(database, str)

        assert isinstance(record, Record)
        assert isinstance(database, str)

        query = ClientQuery(self, UPDATE_RECORD)
        query.ansi(database).add(int(lock)).add(int(actualize))
        query.utf(IRBIS_DELIMITER.join(record.encode()))
        with self.execute(query) as response:
            response.check_return_code()
            result = response.return_code  # Новый максимальный MFN
            if not dont_parse:
                first_line = response.utf()
                text = short_irbis_to_lines(response.utf())
                text.insert(0, first_line)
                record.database = database
                record.parse(text)
            return result

    def write_records(self, records: List[Record]) -> bool:
        """
        Сохранение нескольких записей на сервере.
        Записи могут принадлежать разным базам.

        :param records: Записи для сохранения.
        :return: Результат.
        """
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

        with self.execute(query) as response:
            response.check_return_code()

        return True

    def write_text_file(self, *specification: FileSpecification) -> None:
        """
        Сохранение текстового файла на сервере.

        :param specification: Спецификация (включая текст для сохранения)
        :return: None
        """
        query = ClientQuery(self, READ_DOCUMENT)
        is_ok = False
        for spec in specification:
            assert isinstance(spec, FileSpecification)
            query.ansi(str(spec))
            is_ok = True
        if not is_ok:
            return

        with self.execute(query) as response:
            response.check_return_code()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return exc_type is None

    def __bool__(self):
        return self.connected


__all__ = ['Connection']