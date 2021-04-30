"""
Тест витрины
"""

import asyncio
from pathlib import Path
from json import load as jsl, dump as jsd

from MODS.rest_core.pack_core.RUN.PRE_LAUNCH import APP_INIT

from pack_core.SHOWCASE.constructor import \
    smart_create_showcases, smart_delete_showcases, get_ycl_metadata

parent_folder = Path(__file__).parent.parent
TEST_POST_FILE = parent_folder / \
                 '__test_data' / 'showcase' / '__post_add_update.json'

TEST_DEL_FILE = parent_folder / \
                '__test_data' / 'showcase' / '__delete.json'


async def test_get_metadata():
    """
    Получение метаданных из кликхауса
    """

    out_tables = await get_ycl_metadata()
    with open('z_send_get_meta_ycl.json', 'w', encoding='utf-8') as fb:
        jsd(out_tables, fb, ensure_ascii=False, indent=4)


async def test_processing_post_create():
    """
    Тестирование создания витрины данных
    """
    with open(TEST_POST_FILE, 'r', encoding='utf-8') as file:
        input_data = jsl(fp=file)

    result = await smart_create_showcases(json_data=input_data)
    with open('z_send_insert_showcase.json', 'w', encoding='utf-8') as fb:
        jsd(result, fb, ensure_ascii=False, indent=4)


async def test_processing_delete():
    """
    Тестирование удаления витрины данных
    """
    with open(TEST_DEL_FILE, 'r', encoding='utf-8') as file:
        input_data = jsl(fp=file)

    result = await smart_delete_showcases(json_data=input_data)
    with open('z_send_delete_showcase.json', 'w', encoding='utf-8') as fb:
        jsd(result, fb, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    ioloop = asyncio.get_event_loop()
    # ioloop.run_until_complete(test_processing_post_create())
    # ioloop.run_until_complete(test_processing_delete())
    # ioloop.run_until_complete(test_get_metadata())
