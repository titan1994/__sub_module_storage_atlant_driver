"""
Создание и удаление клиентов.
По сути - обобщение мощностей двух дур машин -
генератора словарей и генератора витрин
"""

from ..main import \
    DEFAULT_META_NAME_SHOWCASE, \
    DEFAULT_META_NAME_DICTS, \
    DEFAULT_META_NAME_ADD_INFO

from ..NSI.tortoise_bridge import bridge_smart_create_dictionaries, bridge_smart_delete_dictionaries
from ..SHOWCASE.constructor import smart_create_showcases, smart_delete_showcases

from ..psql_jsonb.connector import \
    create_or_update_client, get_client, delete_client

"""
Создание/Обновление клиентов
"""


async def smart_create_client(json_data):
    """
    Всё достаточно просто. Мы шаг за шагом вызываем функции по апдейту данных клиентов.
    Отсутствие каких-либо полей в ячейках - не провоцирует их удаление

    Для удаления чего либо нужна голова на плечах. Поэтому это делается отдельно
    """

    summary_report = {}
    for client_key, data_obj in json_data.items():

        report_client = {
            DEFAULT_META_NAME_DICTS: None,
            DEFAULT_META_NAME_SHOWCASE: None,
            DEFAULT_META_NAME_ADD_INFO: None,
        }

        dictionaries = data_obj.get(DEFAULT_META_NAME_DICTS)
        showcases = data_obj.get(DEFAULT_META_NAME_SHOWCASE)

        comment = data_obj.get('human_name')
        if not comment:
            comment = client_key
        add_info = data_obj.get(DEFAULT_META_NAME_ADD_INFO, {})
        add_info['comment'] = comment

        if dictionaries:
            # Создаем/Обновляем словари
            report_client[DEFAULT_META_NAME_DICTS] = await bridge_smart_create_dictionaries(
                data_json={
                    client_key: dictionaries
                }
            )

        if showcases:
            # Создаем/Обновляем витрины
            report_client[DEFAULT_META_NAME_SHOWCASE] = await smart_create_showcases(
                json_data={
                    client_key: showcases
                }
            )

        if add_info:
            # Добавляем/Обновляем поля клиента.
            report_client[DEFAULT_META_NAME_ADD_INFO] = await smart_update_add_info(
                client_key=client_key,
                add_info=add_info
            )

        summary_report[client_key] = report_client

    return summary_report


async def smart_update_add_info(client_key, add_info):
    """
    Обновление полей клиента, отличных от ключевых позиций (витрин и словарей).
    Если клиента нет - он будет создан. На случай если мы просто хотим хранить что-то зачем-то
    """

    ignored_list = [
        DEFAULT_META_NAME_SHOWCASE,
        DEFAULT_META_NAME_DICTS
    ]

    for key in ignored_list:
        if add_info.get(key):
            add_info.pop(key)

    report = {
        'status': True,
        'msg': 'all update'
    }
    try:
        old_client = await get_client(client_key=client_key)
        if old_client:
            old_info = old_client[0]['structureBody']
            for key, value in add_info.items():
                old_info[key] = value
        else:
            old_info = add_info

        await create_or_update_client(
            client_key=old_client,
            add_info=old_info,
            key_info=None
        )

    except Exception as exp:
        report['status'] = False
        report['msg'] = str(exp)

    return report


"""
Удаление клиентов
"""


async def smart_delete_client(json_data):
    """
    Всё достаточно просто. Мы шаг за шагом вызываем функции по удалению данных клиентов.
    Предварительно получив все его метаданные, то есть списки словарей и витрин.

    Жестокая штука но справедливая. Про то что словари этого клиента где-то используются пусть думают
    пользователи прежде чем удалять всё на свете.

    Удаляем кстати даже метаданные в jsonb
    """

    summary_report = {}
    for client_key, in json_data:

        report_client = {
            'status': False,
            'msg': 'client does not exists',
            f'remove_{DEFAULT_META_NAME_DICTS}': None,
            f'remove_{DEFAULT_META_NAME_SHOWCASE}': None,
            'remove_metadata': None
        }

        old_client = await get_client(client_key=client_key)
        if old_client:
            old_info = old_client[0]['structureBody']
        else:
            continue

        dictionaries = old_info.get(DEFAULT_META_NAME_DICTS)
        showcases = old_info.get(DEFAULT_META_NAME_SHOWCASE)

        try:
            # Шаг1 - выносим все словари
            if dictionaries:
                report_client[f'remove_{DEFAULT_META_NAME_DICTS}'] = await bridge_smart_delete_dictionaries(
                    data_json={
                        client_key: dictionaries.keys()
                    }
                )

            # Шаг2 - выносим все витрины
            if showcases:
                report_client[f'remove_{DEFAULT_META_NAME_SHOWCASE}'] = await smart_delete_showcases(
                    json_data={
                        client_key: showcases.keys()
                    }
                )

            # Шаг 3 - удаляем метаданные
            report_client['remove_metadata'] = await delete_client(client_key=client_key)

        except Exception as exp:
            report_client['msg'] = str(exp)

        report_client['msg'] = 'success remove'
        report_client['status'] = True

        summary_report[client_key] = report_client
    return summary_report
