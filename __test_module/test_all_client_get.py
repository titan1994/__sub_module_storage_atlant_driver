import asyncio
from pathlib import Path
from json import load as jsl, dump as jsd

from MODS.rest_core.pack_core.RUN.PRE_LAUNCH import APP_INIT

from pack_core.psql_jsonb.connector import get_all_clients


async def test_get_clients():
    """
    Метаданные клиента
    """
    result = await get_all_clients()
    with open('z_get_jsonb_all_client_metadata.json', 'w', encoding='utf-8') as fb:
        jsd(result, fb, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(test_get_clients())
