"""
Общие положения взаимодействия компонентов ядра
"""

from re import compile as re_compile
from tortoise.backends.base.config_generator import expand_db_url
from MODS.scripts.python.easy_scripts import PROJECT_GENERAL_FOLDER
from MODS.standart_namespace.models import asf

"""
Defaults
"""

DEFAULT_META_NAME_DICTS = 'dictionaries'
DEFAULT_META_NAME_SHOWCASE = 'showcases'
DEFAULT_META_NAME_ADD_INFO = 'add_info'
DEFAULT_OLD_INFO_DICT_META_COLUMNS = 'old_columns'

# Функции которые должны быть вставлены в dictGet - tuple
# dictGet('default.kfx_plan_fact_type_5', 'name', tuple(toInt64(ID))) as Name
DEFAULT_YCL_PRIM_KEY_FUNC_TYPES = {

    # Числа
    'Int64': 'toInt64',
    'Int32': 'toInt32',
    'Int16': 'toInt16',
    'Float64': 'toFloat64',

    # Гуид
    # 'UUID': 'reinterpretAsUUID',
    'UUID': '',
    # Булево
    'UInt8': 'toUInt8',

    # Строки
    'String': 'toString',

    # Все даты в DateTime64
    'DateTime64': 'toDateTime',

    # Частный случай преобразования - наличие доп полей обязательно
    'Decimal64': 'toDecimal64',  #

    # Редкое месиво - в массив
    'Array(Int32)': '',
    'Array(String)': '',
}

"""
JSONB
"""


def get_key_app(name_field=None):
    """
    Имя ключа клиента для поиска в хранилище
    """
    if name_field:
        res = f'ASGI_python_app_{PROJECT_GENERAL_FOLDER.name}_{name_field}'
    else:
        res = f'ASGI_python_app_{PROJECT_GENERAL_FOLDER.name}'

    return res


"""
High level names
"""


def gen_dict_table_name(client_key, name_dict, **kwargs):
    """
    Генерация имени словаря
    """
    general_name = generate_client_object_name(client_key=client_key, name_object=name_dict, **kwargs)
    return generate_db_table_name(f'nsi_{general_name}')


def gen_kafka_topic_name(client_key, name_showcase, **kwargs):
    """
    Генератор имени топика кафки для клиента-потребителя данных в кликхаусе
    """

    return gen_showcase_table_name_kafka(client_key=client_key, name_showcase=name_showcase)


def gen_kafka_group_name(client_key, name_showcase, **kwargs):
    """
    Генератор имени группы потребителей
    """

    general_name = generate_client_object_name(client_key=client_key, name_object=name_showcase, **kwargs)
    return generate_db_table_name(f'showcase_kafka_consumer_{general_name}')


def gen_showcase_table_name_kafka(client_key, name_showcase, **kwargs):
    """
    Генерация имени таблицы очереди данных кафки для витрины данных
    """
    general_name = generate_client_object_name(client_key=client_key, name_object=name_showcase, **kwargs)
    return generate_db_table_name(f'showcase_kafka_queue_{general_name}')


def gen_showcase_table_name_showcase(client_key, name_showcase, **kwargs):
    """
    Генерация имени таблицы витрины данных
    """
    general_name = generate_client_object_name(client_key=client_key, name_object=name_showcase, **kwargs)
    return generate_db_table_name(f'showcase_data_{general_name}')


def gen_showcase_table_name_view(client_key, name_showcase, **kwargs):
    """
    Генерация имени отображения кафки в витрину данных
    """
    general_name = generate_client_object_name(client_key=client_key, name_object=name_showcase, **kwargs)
    return generate_db_table_name(f'showcase_view_{general_name}')


"""
Model names
"""


def generate_db_table_name_from_client(client_key, name_object, **kwargs):
    """
    Добавление префикса проекта к названию таблиц (разрез с точки зрения хранения таблиц в одном месте)
    Предполагается построить сначала имя таблиц по общему правилу
    """
    return generate_db_table_name(
        generate_client_object_name(client_key=client_key, name_object=name_object, **kwargs)
    )


def generate_db_table_name(client_object_name):
    """
    Добавление префикса проекта к названию таблиц (разрез с точки зрения хранения таблиц в одном месте)
    Предполагается что имя таблицы уже построено по правилу generate_client_object_name
    """
    return asf(client_object_name)


def generate_client_object_name(client_key, name_object, **kwargs):
    """
    Генератор имен объектов клиентов.
    Использует регулярные выражения, удаляет лишнее.
    Все имена в нижнем регистре
    """

    return combine_model_name(f'{client_key}_{name_object}')


def combine_model_name(name):
    """
    Причёсываем имя модели регулярным выражением и преобразуем в нижний регистр. Это очень важно!
    Если таблица постгреса с большой буквы - то кликхаус её не найдёт
    Всё остальное касается правил:
        + использовать только латиницу в названиях любых объектов
        + использовать адекватные имена файлов
        + допускается всего один спец символ _
    """
    pattern = re_compile('[^a-zA-z0-9_]')
    res = pattern.sub(string=name, repl='')
    return res.lower()


"""
Connections
"""


def ycl_get_connection_settings(url):
    """
    Настройки подключения к кликхаусу из ссылки для выполнения запроса
    """

    settings_base_dict = expand_db_url(url, True)
    credentials = settings_base_dict['credentials']

    if credentials['password']:
        psw = credentials['password']
    else:
        psw = ''

    settings = {
        'host': credentials['host'],
        'port': credentials['port'],
        'user': credentials['user'],
        'password': psw,
        'database': credentials['database'],
    }
    return settings
