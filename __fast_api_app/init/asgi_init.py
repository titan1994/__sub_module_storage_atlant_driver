"""
Дополнительная инициализация каждого воркера
"""


from GENERAL_CONFIG import GeneralConfig
from os import getenv


def first_init():

    # JSONB Сервис
    GeneralConfig.JAVA_KEY_VALUE_JSONB_URL = getenv('JAVA_KEY_VALUE_JSONB_URL')
    if GeneralConfig.JAVA_KEY_VALUE_JSONB_URL.endswith('/'):
        GeneralConfig.JAVA_KEY_VALUE_JSONB_URL = GeneralConfig.JAVA_KEY_VALUE_JSONB_URL[:-1]

    # Хранилище витрин данных на кликхаусе
    if GeneralConfig.ITS_DOCKER:
        GeneralConfig.CLICKHOUSE_SHOWCASE_URL = getenv('CLICKHOUSE_SHOWCASE_URL_DOCKER')
    else:
        GeneralConfig.CLICKHOUSE_SHOWCASE_URL = getenv('CLICKHOUSE_SHOWCASE_URL_NO_DOCKER')

    if GeneralConfig.CLICKHOUSE_SHOWCASE_URL and GeneralConfig.CLICKHOUSE_SHOWCASE_URL.startswith('clickhouse'):
        GeneralConfig.CLICKHOUSE_SHOWCASE_URL = GeneralConfig.CLICKHOUSE_SHOWCASE_URL.replace('clickhouse', 'postgres', 1)

    # Адрес всех доступных брокеров кафки глазами приложения и глазами кликхауса
    if GeneralConfig.ITS_DOCKER:
        GeneralConfig.KAFKA_URL = getenv('KAFKA_URL_DOCKER')
        GeneralConfig.YCL_KAFKA_URL = GeneralConfig.KAFKA_URL
    else:
        GeneralConfig.KAFKA_URL = getenv('KAFKA_URL_NO_DOCKER')
        YCL_KAFKA_URL_DOCKER = getenv('YCL_KAFKA_URL_DOCKER')
        if YCL_KAFKA_URL_DOCKER:
            GeneralConfig.YCL_KAFKA_URL = YCL_KAFKA_URL_DOCKER
        else:
            GeneralConfig.YCL_KAFKA_URL = GeneralConfig.KAFKA_URL

    print('ASGI INIT')


