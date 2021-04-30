"""
Описывает процедуры генерации/удаления моделей ОРМ (они же НСИ)
"""

from pathlib import Path
from os import getenv
from shutil import rmtree
from tortoise.backends.base.config_generator import expand_db_url

from MODS.scripts.python.jinja import jinja_render_to_file

from MODS.rest_core.pack_core.RUN.__tools.tortoise import DEFAULT_MODEL_FILE_NAME, DEFAULT_PYDANTIC_FILE_NAME
from GENERAL_CONFIG import GeneralConfig
from ..psql_jsonb.connector import create_or_update_client, get_client, \
    json_recombine_key_value, json_recombine_kv_get_info

import MODS.DRIVERS.data_base.async_click_house.ycl as ycl
from MODS.rest_core.pack_core.aerich_proc.mig import update_tables, DEFAULT_FILE_MIGRATION_LOG
from ..main import \
    ycl_get_connection_settings, gen_dict_table_name, \
    DEFAULT_META_NAME_DICTS, DEFAULT_OLD_INFO_DICT_META_COLUMNS, DEFAULT_YCL_PRIM_KEY_FUNC_TYPES

from MODS.standart_namespace.models import get_project_prefix

"""
Конфигурация трансформации типов
"""

# Описание проксирования типов ORM тортоиса в кликхаус
DEFAULT_TORTOISE_TYPE_TO_YCL = {

    # Числа
    'BigIntField': 'Int64',
    'IntField': 'Int32',
    'SmallIntField': 'Int16',
    'FloatField': 'Float64',

    # Гуид
    'UUIDField': 'UUID',

    # Булева нет - хранят в 1м байте
    # https://clickhouse.tech/docs/ru/sql-reference/data-types/boolean/
    'BooleanField': 'UInt8',

    # Все байты в строки
    'BinaryField': 'String',
    'CharField': 'String',
    'JSONField': 'String',
    'TextField': 'String',

    # Все даты в DateTime64
    'DateField': 'DateTime64',
    'DatetimeField': 'DateTime64',
    'TimeDeltaField': 'Int64',

    # Частный случай преобразования - наличие доп полей обязательно
    'DecimalField': '',  # DecimalField(max_digits, decimal_places) to f'Decimal64({precision},{scale})'

    # Редкое месиво - в массив
    'IntEnumField': 'Array(Int32)',
    'CharEnumField': 'Array(String)',
}

DEFAULT_JINJA_PATTERN_ORM = Path(__file__).parent / '_pattern_orm.jinja'
DEFAULT_JINJA_PATTERN_PYD = Path(__file__).parent / '_pattern_pydantic.jinja'


class NSIInputDataError(Exception):
    pass


class NSIProcessingError(Exception):
    pass


class NSIRepairError(Exception):
    pass


"""
Генерация словаря
"""


async def bridge_smart_create_dictionaries(data_json):
    """
    Принимает data/models/api/Dictionary_Settings/_post_add_or_update.json
    Создает или обновляет данные по словарям

    Шаг 1. Файл в ОС - класс питона
    Шаг 2. Отправка миграции в постгрес

    Шаг 3. Создание запроса в кликхаус и его отправка
    Шаг 4. Отправка метаданных

    Всё выполняется оптимизированно. Запросов во внешние источники не делается больше чем надо
    """

    global_response = {
        'dict_input': 0,
        'dict_success_import': 0,
        'orm_processing': None,
        'aerich_state': None,
        'aerich_msg': '',
        'clickhouse_processing': None,
        'downgrade_data': None,
        'downgrade_orm_processing': None,
        'jsonb_processing': None
    }

    for client_key, dicts in data_json.items():
        global_response['dict_input'] = global_response['dict_input'] + len(dicts)

    # Шаг 0 - Получить все необходимые данные для выполнения шагов по созданию словарей
    # Шаг 1 - Выполнить сразу. В создании файлов ORM нет ничего сложного
    step_by_step, response_orm_processing = await process_orm(data_json=data_json)
    global_response['orm_processing'] = response_orm_processing

    # Шаг 2 - Миграция метаданных в постгрес
    res = update_tables()
    if res:
        # Прошла без ошибок
        msg_aerich = 'success migration'
    else:
        # Ошибка мигратора - восстановить исходники
        msg_aerich = f'Aerich migration error. See {DEFAULT_FILE_MIGRATION_LOG}'

        _, response_orm_processing = await process_orm(data_json=data_json, is_repair=True)
        global_response['orm_processing'] = response_orm_processing

    global_response['aerich_state'] = res
    global_response['aerich_msg'] = msg_aerich

    if not res:
        return global_response

    # Шаг 3 - поочерёдная отправка словарей в кликхаус
    clickhouse_processing = {}
    data_repair = {}
    for client_name, data_migrations in step_by_step.items():
        # Для каждого клиента - попытаться заслать все словари

        dict_reports = []
        clickhouse_processing[client_name] = {
            'dict_input': len(data_migrations['ycl'].keys()),
            'dict_success_import': 0,
            'dict_reports': dict_reports
        }

        dict_repair = data_migrations['dict_repair']
        array_repair = {}

        for dict_name, ycl_data in data_migrations['ycl'].items():
            # Словари отправляются поштучно

            report = {
                'status': True,
                'dict_name': dict_name,
                'model_name': ycl_data['model'],
                'msg': 'OK'
            }

            try:
                await app_create_or_update_dict(sql_dict=ycl_data['req'], model_name=ycl_data['model'])
                clickhouse_processing[client_name]['dict_success_import'] = \
                    clickhouse_processing[client_name]['dict_success_import'] + 1

            except Exception as exp:
                # Удаляем словарь из метаданных. Отмечаем словарь на восстановление

                data_migrations['jsonb']['add_info'][DEFAULT_META_NAME_DICTS].pop(dict_name)

                report['status'] = False
                report['msg'] = str(exp)

                array_repair[dict_name] = {
                    'system_name': dict_name,
                    'human_name': dict_repair[dict_name]['dict_comment'],
                    'columns': dict_repair[dict_name]['columns']
                }

            dict_reports.append(report)

        if len(array_repair) > 0:
            # Есть необходимость восстановления
            data_repair[client_name] = array_repair

    # Шаг 4 - оглянись назад мой сын. Ты увидишь чудный мир...
    # Откат назад при сбое в кликхаусе только по указанным словарям
    global_response['clickhouse_processing'] = clickhouse_processing
    if len(data_repair.keys()) > 0:
        _, response_orm_processing = await process_orm(data_json=data_repair, is_repair=True)
        global_response['downgrade_data'] = data_repair
        global_response['downgrade_orm_processing'] = response_orm_processing

        # Ещё раз делаем миграцию
        update_tables()

    # Шаг 5. Для каждого клиента делаем один запрос на обновление
    # Важно - сбой на этом шаге ничего страшного не означает
    # Пояснение - дабы не уйти в циклическое восстановление - лучше отдельно сделать синхронизатор
    # А ещё лучше - просто потом послать запрос повторно.
    # Вцелом, ситуаций когда хранилище недоступно но мы сюда дошли - абсурд.

    jsonb_processing = {}
    for client_name, data_migrations in step_by_step.items():
        # Для каждого клиента - попытаться заслать все словари

        report = {
            'status': True,
            'jsonb_service_info': None,
            'error_msg': ''
        }

        try:
            report['jsonb_service_info'] = await create_or_update_client(
                **data_migrations['jsonb']
            )

            global_response['dict_success_import'] = \
                global_response['dict_success_import'] + \
                clickhouse_processing[client_name]['dict_success_import']

        except Exception as exp:
            report['error_msg'] = str(exp)
            report['status'] = False

        jsonb_processing[client_name] = report

    # dict_success_import
    global_response['jsonb_processing'] = jsonb_processing
    return global_response


async def process_orm(data_json, is_repair=False):
    """
    Получение необходимых данных
    + Шаг 1 - Создание ОРМ классов
    + Восстановление
    """

    response_orm_processing = {}
    step_by_step = {}
    for client_key, dicts in data_json.items():

        response_orm_processing[client_key] = {
            'status': False,
            'dict_input': len(dicts),
            'dict_success': 0,
            'report': []
        }

        if len(dicts) == 0:
            # очень корявый джсон
            continue

        # Инфа по предыдущему состоянию
        # try:

        # Подумалось мне - а накой чёрт обрабатывать что-то если сервис недоступен
        # Пусть валит ошибку и вообще ничего не обрабатывает

        old_client = await get_client(client_key=client_key)
        if old_client:
            old_info = old_client[0]['structureBody']
        else:
            old_info = None

        # except Exception:
        #     # Сервис недоступен и выдаёт ошибку
        #     continue

        ycl = {}
        dict_repair = {}

        if old_info:
            dict_info_old = old_info.get(DEFAULT_META_NAME_DICTS)
            if dict_info_old is None:
                dict_info_old = {}
        else:
            dict_info_old = {}
        meta_info = {DEFAULT_META_NAME_DICTS: dict_info_old}

        for dict_name, dict_obj in dicts.items():

            report_dict = {
                'dict_name': dict_name,
                'success': False,
                'process_data': False,
                'orm_create': False,
                'msg': ''
            }

            try:
                #   Первичное чтение
                key_info = {DEFAULT_META_NAME_DICTS: {dict_name: None}}

                columns = dict_obj['columns']
                dict_comment = dict_obj.get('human_name')
                if not dict_comment:
                    dict_comment = dict_name

                #   пути
                model_name = gen_dict_table_name(client_key=client_key, name_dict=dict_name)
                path_to_models = GeneralConfig.DEFAULT_AERICH_MODEL_APP_PATH / 'NSI'
                client_folder = path_to_models / model_name.replace(get_project_prefix(), '')

                #   Восстановление
                if is_repair:
                    # Попытка восстановления

                    if old_info:
                        # По старым колонкам - просто берём старые колонки и делаем всё что необходимо

                        columns = json_recombine_kv_get_info(
                            key_info=key_info,
                            source_info=old_info,
                            final_key=DEFAULT_OLD_INFO_DICT_META_COLUMNS
                        )

                        if not columns:
                            raise NSIRepairError('columns is empty. repair invalid')
                    else:
                        # Просто удалить файлы (моет быть опасно, только если повредить колонки в хранилище метаданных)
                        # Такое поведение фиксировать ошибками

                        try:
                            rmtree(client_folder)
                        except Exception:
                            pass

                        report_dict['success'] = True
                        raise NSIRepairError(f'folder deleted {client_folder}')

                dict_repair[dict_name] = {
                    'columns': columns,
                    'dict_comment': dict_comment,
                    'name_dict': dict_name,
                    'name_client': client_key,
                }

                report_dict['process_data'] = True

                #   ORM
                create_orm_gen(
                    columns=columns,
                    model_name=model_name,
                    dict_comment=dict_comment,
                    client_folder=client_folder,
                    name_dict=dict_name,
                    name_client=client_key,
                )

                #   Подготовка данных для кликхауса
                ycl_req = create_ycl_req_gen(
                    columns=columns,
                    model_name=model_name
                )

                ycl[dict_name] = {'req': ycl_req, 'model': model_name}
                report_dict['orm_create'] = True

                #   JSONB
                meta_info[DEFAULT_META_NAME_DICTS][dict_name] = {
                    'comment': dict_comment,
                    'columns': columns
                }

                json_recombine_key_value(
                    source_info=old_info,
                    dst_info=meta_info,
                    key_info=key_info,
                    final_key=DEFAULT_OLD_INFO_DICT_META_COLUMNS,
                    final_value=columns
                )

                report_dict['success'] = True
                report_dict['msg'] = 'OK'

                response_orm_processing[client_key]['dict_success'] = \
                    response_orm_processing[client_key]['dict_success'] + 1

            except Exception as exp:
                report_dict['msg'] = str(exp)

            response_orm_processing[client_key]['report'].append(report_dict)

        step_by_step[client_key] = {
            'ycl': ycl,
            'dict_repair': dict_repair,
            'jsonb': {
                'add_info': meta_info,
                'client_key': client_key,
                'key_info': {DEFAULT_META_NAME_DICTS: None}
            }
        }

        response_orm_processing[client_key]['status'] = True

    return step_by_step, response_orm_processing


def create_orm_gen(columns, model_name, dict_comment, client_folder, name_dict, name_client):
    """
    Автогенератор классов ОРМ
    """

    proc_model_name = model_name.replace(get_project_prefix(), '')

    #   ORM
    render_orm = create_jinja_dict_for_python(
        columns=columns,
        model_name=proc_model_name,
        dict_comment=dict_comment,
        name_dict=name_dict,
        name_client=name_client
    )

    #   Pydantic
    render_pydantic = {
        'file_import': DEFAULT_MODEL_FILE_NAME,
        'class_import': proc_model_name
    }

    # Шаг 1 - Автогенератор классов
    jinja_render_to_file(
        src=DEFAULT_JINJA_PATTERN_ORM,
        dst=client_folder / f'{DEFAULT_MODEL_FILE_NAME}.py',
        render=render_orm
    )

    jinja_render_to_file(
        src=DEFAULT_JINJA_PATTERN_PYD,
        dst=client_folder / f'{DEFAULT_PYDANTIC_FILE_NAME}.py',
        render=render_pydantic
    )

    with open(file=client_folder / '__init__.py', mode='w') as fb:
        pass


def create_ycl_req_gen(columns, model_name):
    """
    Генератор запроса на создание словаря в кликхаусе
    """
    render_ycl = create_jinja_dict_for_ycl(
        columns=columns,
        model_name=model_name
    )

    ycl_req = ycl.create_sql_dict(dict_data=render_ycl)
    return ycl_req


def create_jinja_dict_for_python(columns, model_name, dict_comment, name_dict, name_client):
    """
    Создает словарь-рендер для ОРМ-класса на питоне
    """

    jinja_columns = []
    jinja_dict = {
        'name_model': model_name,
        'comment_model': dict_comment,
        'columns': jinja_columns,
        'name_dict': name_dict,
        'name_client': name_client,
    }

    for column in columns:
        column_name = column['system_name']
        data_type = column['data_type']

        if data_type not in DEFAULT_TORTOISE_TYPE_TO_YCL.keys():
            raise NSIInputDataError('INVALID TORTOISE ORM DATA TYPE!')

        column_comment = column.get('human_name', column_name)
        is_primary_key = column.get('is_primary_key', False)
        add_settings = column.get('additional', {})

        if is_primary_key:
            add_settings['pk'] = is_primary_key

        jinja_column = {
            'name': column_name,
            'comment': column_comment,
            'type': data_type,
            'params': add_settings,
        }
        jinja_columns.append(jinja_column)

    return jinja_dict


def create_jinja_dict_for_ycl(columns, model_name):
    """
    Создает словарь-рендер для словаря на кликхаусе
    """

    settings_base_dict = expand_db_url(GeneralConfig.DEFAULT_DB_URI, True)
    credentials = settings_base_dict['credentials']

    if GeneralConfig.ITS_DOCKER:
        host = credentials['host']
    else:
        env_host = getenv('PSQL_IN_DOCKER_HOST')
        if env_host:
            host = env_host
        else:
            host = credentials['host']

    fields_jinja = {}
    func_types = {}

    dict_jinja = {
        'name_dict': model_name,
        'fields': fields_jinja,
        'func': func_types,
        'primary_keys': [],
        'sources': {
            'POSTGRESQL': {
                'port': f"'{credentials['port']}'",
                'host': f"'{host}'",
                'user': f"'{credentials['user']}'",
                'password': f"'{credentials['password']}'",
                'db ': f"'{credentials['database']}'",
                'table': f"'{model_name}'",
            }
        },
        'lay_out': 'complex_key_hashed()',  # Для ключей которые не тупо UINT64
        'life_time': 'MIN 3600 MAX 5400'
    }

    for column in columns:
        column_name = column['system_name']
        data_type = column['data_type']
        column_comment = column.get('human_name', column_name)

        if data_type not in DEFAULT_TORTOISE_TYPE_TO_YCL.keys():
            raise NSIInputDataError('INVALID TORTOISE ORM DATA TYPE!')

        is_primary_key = column.get('is_primary_key', False)
        if is_primary_key:
            dict_jinja['primary_keys'].append(column_name)

        result_type = DEFAULT_TORTOISE_TYPE_TO_YCL[data_type]
        if data_type == 'DecimalField':
            # DecimalField(max_digits, decimal_places)

            add_settings = column['additional']
            precision = add_settings['max_digits']
            scale = add_settings['decimal_places']
            result_type = f'Decimal64({precision},{scale})'

        fields_jinja[column_name] = {
            'type': result_type,
            # 'comment': column_comment,
        }
        func_types[column_name] = DEFAULT_YCL_PRIM_KEY_FUNC_TYPES[result_type]

    return dict_jinja


async def app_create_or_update_dict(sql_dict, model_name):
    """
    Выполняем запрос на создание словаря в кликхаусе.
    Сначала его сбрасываем - потом создаём.

    Операция сброса ошибкой не считается
    """

    conn = ycl_get_connection_settings(GeneralConfig.CLICKHOUSE_SHOWCASE_URL)

    try:
        await ycl.delete_dictionary(conn=conn, name=model_name)
    except Exception as exp:
        pass

    res = await ycl.create_from_sql_dict(conn=conn, sql_req=sql_dict)
    return res


"""
Удаление словаря 
"""


async def bridge_smart_delete_dictionaries(data_json):
    """
    Удаление словарей шаг за шагом.

    Шаг 1. Удалить в кликхаусе
    Шаг 2. Удалить файлы ОРМ на диске
    Шаг 3. Выполнить миграцию
    Шаг 4. Отправить метаданные на сохранение
    """

    summary_result = {
        'dict_input': 0,
        'dict_success_remove': 0,
        'click_house_remove': None,
        'aerich_state': None,
        'aerich_msg': None,
        'jsonb_processing': None
    }

    # Шаги 1-2. Кликхаус, ОРМ
    meta_data_save = {}
    click_house_remove = {}
    cl_repair = []

    none_delete = True
    for client_key, dict_names in data_json.items():

        summary_result['dict_input'] = \
            summary_result['dict_input'] + 1

        report_dict = []
        click_house_remove[client_key] = {
            'status': False,
            'msg': 'invalid input data',
            'dict_input': len(dict_names),
            'dict_success_remove': 0,
            'report_dict': report_dict
        }

        if len(dict_names) == 0:
            # очень корявый джсон
            continue

        # Инфа по предыдущему состоянию
        old_info = None
        old_client = await get_client(client_key=client_key)
        if old_client:
            old_info = old_client[0]['structureBody']

        if not old_info:
            # Мы не можем удалять данные без хранилища
            click_house_remove[client_key]['msg'] = 'invalid old version. abort'
            continue

        old_dict_info = old_info[DEFAULT_META_NAME_DICTS]
        meta_data_save[client_key] = {DEFAULT_META_NAME_DICTS: old_dict_info}

        click_house_remove[client_key]['msg'] = 'remove bad processing. see report_dict field'
        for dict_name in dict_names:

            model_name = gen_dict_table_name(client_key=client_key, name_dict=dict_name)
            path_to_models = GeneralConfig.DEFAULT_AERICH_MODEL_APP_PATH / 'NSI'
            client_folder = path_to_models / model_name.replace(get_project_prefix(), '')

            result_dict = {
                'dict_name': dict_name,
                'model_name': model_name,
                'ycl_remove': False,
                'file_remove': False,
                'msg': 'remove early'
            }

            # Словарь обязательно должен быть в хранилище метаданных.
            # Просто так удалять всё подряд из кликхауса мы не позволим
            if old_dict_info.get(dict_name) is None:
                report_dict.append(result_dict)
                continue

            # А вот теперь как по нотам. Идём в кликхаус - потом сносим файлы.
            # Снести файлы получается всегда - обрабатывать восстановление нет смысла
            try:
                await app_delete_dict(model_name)
                result_dict['ycl_remove'] = True

                rmtree(client_folder)
                result_dict['file_remove'] = True
                result_dict['msg'] = 'success remove'

                # Удаляем из старого джсона - да вот так просто. Да в одну строчку решены все проблемы
                delete_dict = old_dict_info.pop(dict_name)

                cl_repair.append(
                    {
                        'columns': delete_dict['columns'],
                        'model_name': model_name,
                        'dict_comment': delete_dict['comment'],
                        'client_folder': client_folder,
                        'name_dict': dict_name,
                        'name_client': client_key,
                    }
                )

                click_house_remove[client_key]['dict_success_remove'] = \
                    click_house_remove[client_key]['dict_success_remove'] + 1

                # Есть что апдейтнуть
                none_delete = False

            except Exception as exp:
                result_dict['msg'] = str(exp)

            report_dict.append(result_dict)

        if click_house_remove[client_key]['dict_input'] == click_house_remove[client_key]['dict_success_remove']:
            click_house_remove[client_key]['msg'] = 'all dictionaries well be remove'
            click_house_remove[client_key]['status'] = True

    # Шаг 3 - миграция в постгрес (Аерик)
    summary_result['click_house_remove'] = click_house_remove
    if none_delete:
        # ничего не было удалено - изменений нет, либо они не разрешены
        return summary_result

    res = update_tables()
    if res:
        aerich_msg = 'success migration'
    else:
        # Миграция не удалась, а мы набедокурили уже. Откатим изменения
        aerich_msg = f'Aerich migration error. See {DEFAULT_FILE_MIGRATION_LOG}'

        for repair_data in cl_repair:

            try:
                # Вернём на место ОРМ
                create_orm_gen(
                    **repair_data
                )

                # Восстановим значение словаря в кликхаусе
                ycl_req = create_ycl_req_gen(
                    columns=repair_data['columns'],
                    model_name=repair_data['model_name']
                )

                await app_create_or_update_dict(
                    sql_dict=ycl_req,
                    model_name=repair_data['model_name']
                )

            except Exception:
                pass

    summary_result['aerich_state'] = res
    summary_result['aerich_msg'] = aerich_msg

    if not res:
        return summary_result

    # Шаг 4 - применить изменения метаданных
    jsonb_processing = {}
    for client_key, data_dictionaries in meta_data_save.items():

        # Для каждого клиента - попытаться заслать все словари
        report = {
            'status': True,
            'jsonb_service_info': None,
            'error_msg': ''
        }

        try:
            report['jsonb_service_info'] = await create_or_update_client(
                client_key=client_key,
                add_info=data_dictionaries,
                key_info={DEFAULT_META_NAME_DICTS: None}
            )

            summary_result['dict_success_remove'] = \
                summary_result['dict_success_remove'] + \
                click_house_remove[client_key]['dict_success_remove']

        except Exception as exp:
            report['error_msg'] = str(exp)
            report['status'] = False

        jsonb_processing[client_key] = report

    summary_result['jsonb_processing'] = jsonb_processing
    return summary_result


async def app_delete_dict(name):
    """
    Удаление словаря
    """

    conn = ycl_get_connection_settings(GeneralConfig.CLICKHOUSE_SHOWCASE_URL)
    res = await ycl.delete_dictionary(conn=conn, name=name)

    return res
