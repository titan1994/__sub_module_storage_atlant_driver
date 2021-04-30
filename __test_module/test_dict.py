"""
Тест словарей
"""

import asyncio
from pathlib import Path
from json import load as jsl, dump as jsd

from MODS.rest_core.pack_core.RUN.PRE_LAUNCH import APP_INIT
from pack_core.NSI.tortoise_bridge import \
    bridge_smart_create_dictionaries, bridge_smart_delete_dictionaries

parent_folder = Path(__file__).parent.parent
TEST_POST_FILE = parent_folder / \
                 '__test_data' / 'dictionary' / '__post_add_update.json'

TEST_DEL_FILE = parent_folder / \
                '__test_data' / 'dictionary' / '__delete.json'


async def test_processing_post_create():
    """
    Тестирование создания словаря
    """
    with open(TEST_POST_FILE, 'r', encoding='utf-8') as file:
        input_data = jsl(fp=file)

    result = await bridge_smart_create_dictionaries(json_data=input_data)
    with open('z_send_insert_dict.json', 'w', encoding='utf-8') as fb:
        jsd(result, fb, ensure_ascii=False, indent=4)


async def test_processing_delete():
    """
    Тестирование удаления словаря
    """

    with open(TEST_DEL_FILE, 'r', encoding='utf-8') as file:
        input_data = jsl(fp=file)

    result = await bridge_smart_delete_dictionaries(json_data=input_data)
    with open('z_send_delete_dict.json', 'w', encoding='utf-8') as fb:
        jsd(result, fb, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    """
    Тест
    """

    ioloop = asyncio.get_event_loop()
    # ioloop.run_until_complete(test_processing_post_create())
    # ioloop.run_until_complete(test_processing_delete())
