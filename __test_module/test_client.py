"""
Узри человек мощь системы хранилища
"""

import asyncio
from pathlib import Path
from json import load as jsl, dump as jsd

from MODS.rest_core.pack_core.RUN.PRE_LAUNCH import APP_INIT
from pack_core.HEART.client_api import smart_create_client, smart_delete_client
from pack_core.psql_jsonb.connector import get_client

parent_folder = Path(__file__).parent.parent
TEST_POST_FILE = parent_folder / \
                 '__test_data' / 'client' / '__post_add_update.json'

TEST_DEL_FILE = parent_folder / \
                '__test_data' / 'client' / '__delete.json'


async def test_processing_post_create():
    """
    Тестирование создания/обновления клиентов
    """
    with open(TEST_POST_FILE, 'r', encoding='utf-8') as file:
        input_data = jsl(fp=file)

    result = await smart_create_client(json_data=input_data)
    with open('z_send_insert_showcase.json', 'w', encoding='utf-8') as fb:
        jsd(result, fb, ensure_ascii=False, indent=4)


async def test_processing_delete():
    """
    Тестирование удаления клиентов
    """
    with open(TEST_DEL_FILE, 'r', encoding='utf-8') as file:
        input_data = jsl(fp=file)

    result = await smart_delete_client(json_data=input_data)
    with open('z_send_delete_showcase.json', 'w', encoding='utf-8') as fb:
        jsd(result, fb, ensure_ascii=False, indent=4)


async def test_get_client(client_key):
    """
    Метаданные клиента
    """
    result = await get_client(client_key=client_key)
    with open('z_get_jsonb_client_metadata.json', 'w', encoding='utf-8') as fb:
        jsd(result, fb, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(test_processing_post_create())
    # ioloop.run_until_complete(test_processing_delete())
    # ioloop.run_until_complete(test_get_client(client_key='general_nsi'))
