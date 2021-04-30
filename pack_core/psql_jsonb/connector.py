"""
Подключение к внешнему сервису key-value psql+jsonb+java
"""
from uuid import uuid4

from MODS.DRIVERS.network.async_http import send_get, send_post, send_delete
from ..main import get_key_app
from GENERAL_CONFIG import GeneralConfig

DEFAULT_client_key_NAME = 'client_key'
DEFAULT_APP_KEY_NAME = 'ASGI_python_showcase_app_name'


async def get_client(client_key):
    """
    Получает данные текущего клиента
    """
    key_name = get_key_app(DEFAULT_client_key_NAME)
    link_client = f'{GeneralConfig.JAVA_KEY_VALUE_JSONB_URL}/export/structure/{key_name}/{client_key}'
    res = await send_get(url=link_client)
    if res.status_code != 200:
        # info = res.text
        return None
    return res.json()


async def delete_client(client_key):
    """
    Удаляет данные текущего клиента
    """
    key_name = get_key_app(DEFAULT_client_key_NAME)
    link_client = f'{GeneralConfig.JAVA_KEY_VALUE_JSONB_URL}/export/structure/{key_name}/{client_key}'
    res = await send_delete(url=link_client)
    if res.status_code != 200:
        # info = res.text
        return None
    return res.json()


async def create_client(client_key, client_info=None, guid_update=None):
    """
    Создание клиента в хранилище метаданных
    """

    name_app = get_key_app()
    key_name = get_key_app(DEFAULT_client_key_NAME)

    if guid_update:
        guid_value = guid_update
    else:
        guid_value = uuid4()

    client_obj = {
        "structureMetadata": {
            "guid": f"{guid_value}",
            "type": "alpha",
            DEFAULT_APP_KEY_NAME: name_app,
            key_name: client_key
        }
    }
    if client_info:
        client_obj['structureBody'] = client_info
    else:
        client_obj['structureBody'] = {}

    list_data_to_service = [
        client_obj
    ]

    link_client = f'{GeneralConfig.JAVA_KEY_VALUE_JSONB_URL}/import-or-update/common'

    res = await send_post(url=link_client, data_body=list_data_to_service)
    if res.status_code != 200:
        # info = res.text
        return None

    return list_data_to_service


async def create_or_update_client(client_key, add_info=None, key_info=None):
    """
    Создать или обновить клиента

    client_key - ключ клиента строкой или заранее полученный клиент
    """

    if isinstance(client_key, str):
        # Ключ клиента
        client = await get_client(client_key=client_key)
        client_name = client_key
    else:
        # Метаданные клиента
        client = client_key
        client_name = client[0]['structureMetadata'][get_key_app(DEFAULT_client_key_NAME)]

    if client:
        guid_update = client[0]['structureMetadata']['guid']
        if key_info:
            # обновляется только часть информации по клиенту
            old_info = client[0]['structureBody']
            json_update_key_value(
                old_info=old_info,
                key_info=key_info,
                add_info=add_info
            )

            info = old_info
        else:
            info = add_info

        res = await create_client(
            client_key=client_name,
            client_info=info,
            guid_update=guid_update
        )
    else:
        # Создать клиента со всей информацией

        if key_info:
            # Клиент создается не с верхнего уровня - указать откуда в ключе

            info = {}
            json_update_key_value(
                old_info=info,
                key_info=key_info,
                add_info=add_info
            )
        else:
            info = add_info

        res = await create_client(
            client_key=client_name,
            client_info=info
        )
    if key_info:
        get_this_client = json_recombine_kv_get_info(
            key_info=key_info,
            source_info=res[0]['structureBody'],
            final_key=None
        )
    else:
        get_this_client = res

    return get_this_client


def json_update_key_value(old_info, add_info, key_info):
    """
    Рекурсивный апгрейд
    """
    for key, value in key_info.items():

        if value:

            if old_info.get(key):
                info = old_info[key]
            else:
                old_info[key] = add_info[key]
                info = old_info[key]

            json_update_key_value(
                old_info=info,
                key_info=value,
                add_info=add_info[key]
            )
        else:
            old_info[key] = add_info[key]


def json_recombine_key_value(source_info, dst_info, key_info, final_key, final_value):
    """
    Перераспределение значения из старого в новое
    """

    if source_info:
        res_value = json_recombine_kv_get_info(
            key_info=key_info,
            source_info=source_info,
            final_key=final_key
        )

        if res_value is None:
            res_value = final_value
    else:
        res_value = final_value

    json_recombine_kv_set_info(
        dst_info=dst_info,
        key_info=key_info,
        final_key=final_key,
        final_value=res_value
    )


def json_recombine_kv_get_info(key_info, source_info, final_key):
    """
    Получение значения ключа в глубину
    """

    for key, value in key_info.items():
        if source_info.get(key) is None:
            return None

        if value:
            return json_recombine_kv_get_info(
                key_info=value,
                source_info=source_info[key],
                final_key=final_key
            )
        else:
            if final_key:
                return source_info[key].get(final_key)
            else:
                return source_info[key]


def json_recombine_kv_set_info(dst_info, key_info, final_key, final_value):
    """
    Установление значения ключа в глубину
    """

    for key, value in key_info.items():
        if dst_info.get(key) is None:
            break

        if value:
            json_recombine_kv_set_info(
                key_info=value,
                dst_info=dst_info[key],
                final_key=final_key,
                final_value=final_value
            )
        else:
            dst_info[key][final_key] = final_value
