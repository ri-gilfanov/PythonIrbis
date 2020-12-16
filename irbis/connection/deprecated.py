from typing import TYPE_CHECKING
from irbis.connection.sync_handlers import SyncHandlers
from irbis.records import Record

if TYPE_CHECKING:
    from typing import Optional


class DeprecatedConnMixin(SyncHandlers):
    def read_raw_record(self, mfn: int, version: int = 0)\
            -> 'Optional[Record]':
        """
        Чтение сырой записи с сервера.

        :param mfn: MFN записи.
        :param version: версия
        :return: Загруженная с сервера запись.
        """
        return self.read_record(mfn, version)

    def write_raw_record(self, record: Record, lock: bool = False,
                         actualize: bool = True) -> int:
        """
        Сохранение записи на сервере.

        :param record: Запись
        :param lock: Оставить запись заблокированной?
        :param actualize: Актуализировать запись?
        :return: Новый максимальный MFN.
        """
        return self.write_record(record, lock, actualize, dont_parse=True)
