import datetime


class LogSerializable:
    """ Объект для упрощения маппинга при парсинге логов """

    def __init__(self, line):
        """ Конструктор. Все атрибуты - частные """
        self.__time = datetime.datetime.fromisoformat(line[5])
        self.__login = line[4]
        self.__ip = line[2]
        self.__type = line[3]

    @property
    def time(self):
        return self.__time

    @property
    def login(self):
        return self.__login

    @property
    def ip(self):
        return self.__ip

    @property
    def type(self):
        return self.__type
