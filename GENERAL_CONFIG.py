"""
Основная конфигурация проекта.
Собирает в себе все конфигурации подмодулей
"""

from MODS.scripts.python.easy_scripts import PROJECT_GENERAL_FOLDER as general_path
from multiprocessing import cpu_count

from abc import ABC, abstractmethod
from enum import Enum


class AppMode(Enum):
    """
    Режим развертки приложения продуктив/отладка
    """

    debug = 'debug'
    production = 'production'


class NoObjectMixin(ABC):
    """
    Защита от супер умных - экземпляры рождать нельзя
    """

    @abstractmethod
    def __no_object(self):
        pass


class FastApiSettings(NoObjectMixin):
    """
    Настройка фаст-апи. Адаптация под проект
    """
    DEFAULT_AERICH_CFG_PATH = 'MODS.rest_core.pack_core.aerich_proc.aerich_conf.TORTOISE_ORM'
    DEFAULT_DB_URI = None


class FastApiConfig(FastApiSettings):
    """
    Статичная конфигурация фаст-апи
    """

    # Основная папка с моделями тортоиса
    DEFAULT_AERICH_MODEL_PACK_PATH = '__fast_api_app.models'

    # путь к папке с миграциями для тортоиса
    DEFAULT_AERICH_MIGR_PATH = general_path / '__migrations' / 'aerich'

    # путь к файлу хранения конфигурации .ini
    DEFAULT_AERICH_INI_FILE = general_path / 'aerich.ini'
    DEFAULT_AERICH_INI_PATH = general_path / '__migrations' / 'aerich' / 'aerich.ini'

    # папка в которую смотрит анализатор моделей и ищет их там
    DEFAULT_AERICH_MODEL_APP_PATH = \
        general_path / DEFAULT_AERICH_MODEL_PACK_PATH.replace('.', '/') / 'general'


class AtlantStorageCfg:
    """
    Конфигурация хранилища
    """
    JAVA_KEY_VALUE_JSONB_URL = None
    CLICKHOUSE_SHOWCASE_URL = None
    KAFKA_URL = None
    YCL_KAFKA_URL = None


class GeneralConfig(FastApiConfig, AtlantStorageCfg):
    """
    Общая конфа - она импортируется по проекту
    """
    PROJECT_NAME = 'ЧАСТЬ СЕРВИСА ХРАНИЛИЩА. НЕ ТЕСТИТЬ ТУТ!'
    DEFAULT_APP_MODE = AppMode.debug
    DEFAULT_PORT = 5111  # Порт

    ITS_DOCKER = None
    PROJECT_GENERAL_FOLDER = general_path
    DEFAULT_WORKER_COUNT = cpu_count() + 1
