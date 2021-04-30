from uuid import uuid4

from MODS.scripts.python.jinja import jinja_render_str_to_str
from MODS.scripts.python.dict_proc import get_dict_difference, recombine_dict

from GENERAL_CONFIG import GeneralConfig
from ..main import \
    gen_kafka_topic_name, \
    gen_kafka_group_name, \
    gen_showcase_table_name_kafka, \
    gen_showcase_table_name_showcase, \
    gen_showcase_table_name_view, \
    ycl_get_connection_settings, \
    gen_dict_table_name, \
    DEFAULT_META_NAME_SHOWCASE, \
    DEFAULT_YCL_PRIM_KEY_FUNC_TYPES

import MODS.DRIVERS.data_base.async_click_house.ycl as ycl

from MODS.DRIVERS.kafka_proc.driver import KafkaAdmin, KafkaProducerConfluent

from ..psql_jsonb.connector import \
    create_or_update_client, get_client

from MODS.standart_namespace.models import get_project_prefix


"""
Создание витрины данных + Мега апдейт 
"""


class CreateShowcaseError(Exception):
    pass


async def smart_create_showcases(json_data):
    """
    Создание витрин данных шаг за шагом. Showcase_Settings/_post_add_or_update.json

    Шаг 1. Получить метаданные клиента
    Инфо. Каждая витрина - это отдельный набор интеграционных таблиц и свой топик кафки
        + Проверяем наличие топика - если он есть - мы его не пересоздаём и не меняем.
        Такие сложные операции администрирования кафки пока что не поддерживаем на данном этапе
        + Все остальные настройки, которые касаются кликхауса менять можно.

    Шаг 2. Создаем топик кафки если его нет. Если он есть - не трогаем его.
        И тут самое важное - это количество партиций. Надо прям решить сколько их будет
        Админить только средствами самой кафки или будущим ПО

    Шаг 3. Создаем таблицу кликхауса, если её нет. Пробуем получить по ней метаданные.
        Если есть метаданные - то таблица нуждается в реконструкции (обновление колонок)
        Таблица кафки также нуждается в реконструкции как и материализованное представление.
        Отношения со словарём дают отдельный характер реконструкции

        Если необходимо изменить целевую таблицу с помощью ALTER,
        то материализованное представление рекомендуется отключить (А МЫ ПРОСТО УДАЛИМ И ЗАНОВО СОЗДАДИМ),
        чтобы избежать несостыковки между целевой таблицей и данными от представления.
        https://clickhouse.tech/docs/ru/engines/table-engines/integrations/kafka/#table_engine-kafka-creating-a-table

    При сбое ничего не откатывать назад. Так как фактически - предпоследний шаг - сбой кликхауса ни
    к чему не приведёт страшному. Ну появиться топик в кафке. Ну и что. Удалить топик из кафки - целая наука.
    Проще послать ещё раз запрос. Чем откатить кафку назад

    """

    global_response = {
        'show_case_input': 0,
        'show_case_success_import': 0,
        'report_show_cases': {},
        'new_meta_show_cases': {}
    }

    optional_key_list = [
        'index',
        'order_by',
        'partition_by',
        'primary_keys',
        'sample_by',
        'ttl',
        'settings',
    ]

    conn_cl = ycl_get_connection_settings(
        GeneralConfig.CLICKHOUSE_SHOWCASE_URL
    )

    with KafkaProducerConfluent() as ka:
        list_of_topics = ka.get_list_topics()

    if not list_of_topics:
        list_of_topics = []

    for client_key, data_showcases in json_data.items():

        report_processing = {}
        global_response['report_show_cases'][client_key] = {
            'status': True,
            'msg': 'OK',
            'showcase_input': len(data_showcases),
            'showcase_success_insert': 0,
            'report': report_processing
        }

        if len(data_showcases) == 0:
            # очень корявый джсон
            global_response['report_show_cases'][client_key]['status'] = False
            global_response['report_show_cases'][client_key]['msg'] = 'invalid input data'

            continue

        # Получаем метаданные
        old_client = await get_client(client_key=client_key)
        if old_client:
            old_info = old_client[0]['structureBody']
        else:
            old_info = None

        if old_info:
            dict_info_old = old_info.get(DEFAULT_META_NAME_SHOWCASE)
            if dict_info_old is None:
                dict_info_old = {}
        else:
            dict_info_old = {}

        meta_info = {DEFAULT_META_NAME_SHOWCASE: dict_info_old}

        # Обрабатываем запрос на создание/обновление
        for showcase_name, showcase_obj in data_showcases.items():

            report_showcase = {
                'success': False,
                'process_input_data': False,
                'create_or_update_topic': False,
                'create_or_update_queue': False,
                'create_or_update_showcase': False,
                'create_or_update_view': False,
                'msg': 'OK'
            }
            try:
                # Одна большая попытка Step-By-Step

                """
                Парсинг входных данных
                """

                global_response['show_case_input'] = \
                    global_response['show_case_input'] + 1

                # showcase_name = showcase_obj['system_name']
                columns = showcase_obj['columns']
                column_repair_for_jinja(columns)

                info_old_showcase = dict_info_old.get(showcase_name)

                engine = showcase_obj.get('engine')
                if not engine:
                    engine = 'MergeTree()'

                columns_mview = {}
                for key in columns.keys():
                    # Для материализованных представлений мы всегда делаем сквозной проброс колонок от кафки в таргет
                    columns_mview[key] = key

                column_kafka = columns.copy()

                showcase_comment = showcase_obj.get('human_name')
                if not showcase_comment:
                    showcase_comment = showcase_name

                kafka_settings_queue = showcase_obj.get('kafka_settings_queue')

                # Имена
                relation_to_dict = showcase_obj.get('relation_dicts')
                list_dict_tables = []
                if relation_to_dict:
                    # Воссоздание имён словарей
                    for key, data_dict in relation_to_dict.items():
                        dict_key = data_dict.get('client_name')
                        if not dict_key:
                            dict_key = client_key

                        table_dict = adaptation_dict_name(client_key=dict_key, key_name=key)
                        list_dict_tables.append(table_dict)

                topic_name = gen_kafka_topic_name(
                    client_key=client_key,
                    name_showcase=showcase_name
                )

                group_name = gen_kafka_group_name(
                    client_key=client_key,
                    name_showcase=showcase_name
                )

                table_showcase_kafka_queue = gen_showcase_table_name_kafka(
                    client_key=client_key, name_showcase=showcase_name
                )

                table_showcase_data = gen_showcase_table_name_showcase(
                    client_key=client_key, name_showcase=showcase_name
                )

                table_showcase_view = gen_showcase_table_name_view(
                    client_key=client_key, name_showcase=showcase_name
                )

                report_showcase['process_input_data'] = True

                # получение метаданных таблиц как хороший способ понять работает кликхаус или нет
                meta_rows_data = await get_ycl_metadata(table_filter=[
                    table_showcase_kafka_queue,
                    table_showcase_data,
                    table_showcase_view,
                    *list_dict_tables
                ])

                metadata_tables = {}
                if meta_rows_data:
                    # Слегка причешем метаданные
                    for obj_meta in meta_rows_data:
                        metadata_tables[obj_meta['name']] = obj_meta['attributes']

                # Дополнить колонки колонками словаря
                ycl_generate_column_showcase(
                    client_key=client_key,
                    column_input=columns,
                    relation_dicts=relation_to_dict,
                    meta_data=metadata_tables
                )

                # Причесать настройки кафки
                kafka_settings_processing(
                    kafka_settings_queue=kafka_settings_queue,
                    topic_name=topic_name,
                    group_name=group_name
                )

                # Причесать настройки создания таблиц
                target_table_data = {
                    'table_name': table_showcase_data,
                    'columns': columns,
                    'engine': engine
                }

                for opt_key in optional_key_list:
                    # Добавление опциональных параметров если они есть
                    opt_data = showcase_obj.get(opt_key)
                    if opt_data:
                        if opt_key == 'settings':
                            ycl_repair_settings(settings=opt_data)
                        target_table_data[opt_key] = opt_data

                # Подготовка новых метаданных.
                info_new_showcase = {
                    'comment': showcase_comment,
                    'target_table': target_table_data,
                    'kafka_settings_queue': kafka_settings_queue,
                    'kafka_topic_name': topic_name,
                    'kafka_group_consumer_name': group_name,
                    'ycl_table_showcase_kafka_queue': table_showcase_kafka_queue,
                    'ycl_table_showcase_data': table_showcase_data,
                    'ycl_table_showcase_view': table_showcase_view,
                }

                # Анализ старых метаданных и новых на предмет различий в колонках и настройках
                proc_kafka_settings_key = None
                proc_target_settings_key = None
                diff_column_modify = None
                diff_index_modify = None

                if info_old_showcase:
                    _, proc_kafka_settings_key = recombine_dict(
                        old=info_old_showcase.get('kafka_settings_queue'),
                        new=info_new_showcase.get('kafka_settings_queue'),
                    )

                    _, proc_target_settings_key = recombine_dict(
                        old=info_old_showcase.get('target_table').get('settings'),
                        new=info_new_showcase.get('target_table').get('settings'),
                    )

                    diff_column_modify = get_dict_difference(
                        old=info_old_showcase.get('target_table').get('columns'),
                        new=info_new_showcase.get('target_table').get('columns'),
                    )

                    diff_index_modify = get_dict_difference(
                        old=info_old_showcase.get('target_table').get('index'),
                        new=info_new_showcase.get('target_table').get('index'),
                    )

                # Решение о создании нового топика. Если кликхаус рухнет потом - просто повторим запрос в другой раз
                if topic_name not in list_of_topics:
                    if kafka_settings_queue is None:
                        raise CreateShowcaseError('topic is not found. for create it we need kafka settings!')

                    kafka_num_consumers = get_num_partition_consumers(kafka_settings_queue)
                    with KafkaAdmin() as ka:
                        ka.create_topic(topics={'name': topic_name, 'num_partitions': kafka_num_consumers})

                report_showcase['create_or_update_topic'] = True

                """
                По шагам создаем или изменяем витрину данных
                """

                # Шаг 0. Если есть представление - удаляем (оно и отсоединиться как раз).
                # Его проще создать заново, чем апдейтнуть
                meta_view = metadata_tables.get(table_showcase_view)
                if meta_view:
                    await ycl.delete_table(conn=conn_cl, name=table_showcase_view)

                # Шаг +1. Создаем таблицу кафки. Если она есть - пробуем поменять колонки и настройки формата данных
                meta_kafka = metadata_tables.get(table_showcase_kafka_queue)
                create_table = True

                if meta_kafka:
                    # Замена колонок и настроек

                    if diff_column_modify:
                        # КОЛОНКИ
                        # Заменили ёпть... Удаление one love
                        # DB::Exception: Alter of type 'ADD COLUMN' is not supported by storage Kafka

                        await ycl.delete_table(conn=conn_cl, name=table_showcase_kafka_queue)
                        report_showcase['queue_report_column_update'] = 'success remove kafka queue'

                        # report_showcase['queue_report_column_update'] = await ycl_column_update(
                        #     conn=conn_cl,
                        #     table_name=table_showcase_kafka_queue,
                        #     column_change=diff_column_modify,
                        #     column_dict=info_new_showcase.get('target_table').get('columns'),
                        #     key_ignored=['is_dict'],
                        # )

                    elif proc_kafka_settings_key:
                        # НАСТРОЙКИ
                        create_table = False
                        report_showcase['queue_report_settings_update'] = await ycl_settings_update(
                            conn=conn_cl,
                            table_name=table_showcase_kafka_queue,
                            settings_dict=info_new_showcase.get('kafka_settings_queue'),
                            settings_key_change=proc_kafka_settings_key,
                        )

                if create_table:
                    # Создание таблицы кафки с заданными настройками
                    kafka_table_data = {
                        'table_name': table_showcase_kafka_queue,
                        'columns': column_kafka,
                        'engine': 'Kafka()',
                        'settings': kafka_settings_queue
                    }

                    await ycl.create_table(conn=conn_cl, table_data=kafka_table_data)

                report_showcase['create_or_update_queue'] = True

                # Шаг + 1. Создаем целевую таблицу. Если она есть - производим анализ того что поменять.
                meta_showcase_data = metadata_tables.get(table_showcase_data)
                if meta_showcase_data:
                    # Замена настроек и колонок (в будущем других параметров таблицы)

                    if proc_target_settings_key:
                        # Настройки
                        report_showcase['target_report_settings_update'] = await ycl_settings_update(
                            conn=conn_cl,
                            table_name=table_showcase_data,
                            settings_dict=info_new_showcase.get('target_table').get('settings'),
                            settings_key_change=proc_target_settings_key,

                        )

                    if diff_column_modify:
                        # Колонки

                        report_showcase['target_report_column_update'] = await ycl_column_update(
                            conn=conn_cl,
                            table_name=table_showcase_data,
                            column_change=diff_column_modify,
                            column_dict=info_new_showcase.get('target_table').get('columns'),
                        )
                else:
                    # Создать целевую таблицу хранения данных
                    await ycl.create_table(conn=conn_cl, table_data=target_table_data)

                report_showcase['create_or_update_showcase'] = True

                # Шаг + 1. Создать материализованное представление
                mview_table_data = {
                    'view_name': table_showcase_view,
                    'table_src': table_showcase_kafka_queue,
                    'table_dst': table_showcase_data,
                    'columns': columns_mview,
                }

                await ycl.create_materialized_view(conn=conn_cl, view_data=mview_table_data)

                report_showcase['create_or_update_view'] = True

                # Шаг последний. Сохранить метаданные клиента
                meta_info[DEFAULT_META_NAME_SHOWCASE][showcase_name] = info_new_showcase

                global_response['show_case_success_import'] = \
                    global_response['show_case_success_import'] + 1

                global_response['report_show_cases'][client_key]['showcase_success_insert'] = \
                    global_response['report_show_cases'][client_key]['showcase_success_insert'] + 1

                report_showcase['success'] = True

            except Exception as exp:
                report_showcase['msg'] = str(exp)

                global_response['report_show_cases'][client_key]['status'] = False
                global_response['report_show_cases'][client_key]['msg'] = 'see report'

            # Отправка по одному клиенту метаданных по всем витринам
            report_processing[showcase_name] = report_showcase

        try:
            if global_response['show_case_success_import'] > 0:
                key_info = {DEFAULT_META_NAME_SHOWCASE: None}

                global_response['new_meta_show_cases'][client_key] = await create_or_update_client(
                    client_key=client_key,
                    add_info=meta_info,
                    key_info=key_info
                )
        except Exception as exp:
            pass

    return global_response


async def ycl_column_update(conn, table_name, column_change, column_dict):
    """
    Обновление колонок. Тоесть точное перевоплощение их структуры в новый указанный формат.
    """

    modify_reporting = {}

    # Удаление колонок
    remove_data = column_change.get('remove')
    if remove_data:
        for key, val in remove_data.items():
            report = {
                'status': True,
                'action': 'remove',
                'msg': 'OK',
            }
            try:
                await ycl.column_delete(conn=conn, table=table_name, name=key)
            except Exception as exp:
                report['status'] = False
                report['msg'] = str(exp)

            modify_reporting[key] = report

    # Добавление колонок
    add_data = column_change.get('add')
    if add_data:
        for key, column in add_data.items():
            report = {
                'status': True,
                'action': 'add',
                'msg': 'OK',
            }
            try:
                await ycl.column_add(
                    conn=conn,
                    table=table_name,
                    name=key,
                    type=column['type'],
                    expr=column['dma'],
                )
            except Exception as exp:
                report['status'] = False
                report['msg'] = str(exp)

            try:
                await ycl.column_comment(
                    conn=conn,
                    table=table_name,
                    name=key,
                    comment=column['comment'],
                )
            except Exception:
                pass

            modify_reporting[key] = report

    # Изменение колонок
    change_data = column_change.get('change')
    if change_data:
        # Анализ изменений
        column_general_change = {}
        for key, ch_dat in change_data.items():

            key_split = key.split('.')
            key_column = key_split[0]
            change_factor = column_general_change.get(key_column, {})

            if change_factor.get('type_change') in ['new_type', 'new_name']:
                # Если тип изменений уже задан как новый тип или новое имя - не делать остальные изменения
                continue

            if len(key_split) == 2:
                # Внутреннее изменение параметров колонки
                new_data = column_dict.get(key_column)
                key_parameter = key_split[1]

                if key_parameter == 'type':
                    # Изменение типа. Должно сопровождаться переименовыванием колонки со старым типом.
                    # И созданием совершенно новой колонки
                    change_factor['type_change'] = 'new_type'
                    old_type = list(ch_dat.keys())[0]

                    change_factor['data'] = {
                        'old_name': f'old_{key_column}_{old_type}_{uuid4()}'.replace('-', '_'),
                        'new_column': new_data,
                    }

                elif key_parameter == 'new_name':
                    # Переименовывание колонки. Сопровождать изменением комментария

                    change_factor['type_change'] = 'new_name'
                    change_factor['data'] = {
                        key_parameter: new_data[key_parameter],
                        'comment': new_data['comment'],
                    }
                else:
                    # Изменение TTL и прочего.
                    change_factor['type_change'] = 'parameters'
                    list_parameters = change_factor.get('data', {})
                    list_parameters['type'] = new_data['type']  # Тип меняется по другому
                    list_parameters[key_parameter] = new_data[key_parameter]
                    change_factor['data'] = list_parameters

            column_general_change[key_column] = change_factor

        # Применение проанализированных изменений
        for key_column, change_factor in column_general_change.items():

            type_change = change_factor['type_change']
            data_change = change_factor['data']

            report = {
                'status': True,
                'action': type_change,
                'msg': 'OK',
            }

            try:
                if type_change == 'new_type':
                    # Изменение типа (Стоит дорого). Проще поменять имя и создать колонку с новым типом

                    await ycl.column_rename(
                        conn=conn,
                        table=table_name,
                        old_name=key_column,
                        new_name=data_change['old_name'],
                    )

                    await ycl.column_add(
                        conn=conn,
                        table=table_name,
                        name=key_column,
                        type=data_change['new_column']['type'],
                        expr=data_change['new_column']['dma'],
                    )

                    try:
                        await ycl.column_comment(
                            conn=conn,
                            table=table_name,
                            name=key_column,
                            comment=data_change['new_column']['comment'],
                        )
                    except Exception:
                        pass

                elif type_change == 'new_name':
                    # Изменить имя и комментарий. Сначала комментарий

                    try:
                        await ycl.column_comment(
                            conn=conn,
                            table=table_name,
                            name=key_column,
                            comment=data_change['comment'],
                        )
                    except Exception:
                        pass

                    new_name = data_change['new_name']
                    await ycl.column_rename(
                        conn=conn,
                        table=table_name,
                        old_name=key_column,
                        new_name=new_name,
                    )

                    column_dict[new_name] = column_dict.pop(key_column)
                else:
                    # Простая модификация. Если параметр - это комментарий - то его отдельно

                    await ycl.column_modify(
                        conn=conn,
                        table=table_name,
                        name=key_column,
                        type=data_change['type'],
                        expr=data_change.get('dma'),
                        ttl=data_change.get('ttl'),
                    )

                    if data_change.get('comment'):
                        try:
                            await ycl.column_comment(
                                conn=conn,
                                table=table_name,
                                name=key_column,
                                comment=data_change['comment'],
                            )
                        except Exception:
                            pass

            except Exception as exp:
                report['status'] = False
                report['msg'] = str(exp)

            modify_reporting[key_column] = report

    return modify_reporting


async def ycl_settings_update(conn, table_name, settings_dict, settings_key_change):
    """
    Тут просто так совпало, что можно пробежать циклом и просто подставить новые данные в MODIFY
    """
    modify_reporting = {}

    for key in settings_key_change:
        report = {
            'status': True,
            'msg': 'OK'
        }
        try:
            await ycl.modify_settings(
                conn=conn,
                table=table_name,
                settings=key,
                value=settings_dict[key]
            )
        except Exception as exp:
            report['status'] = False
            report['msg'] = str(exp)

        modify_reporting[key] = report

    return modify_reporting


def kafka_settings_processing(kafka_settings_queue, topic_name, group_name):
    """
    Процессор настроек кафки
    """

    if kafka_settings_queue is None:
        kafka_settings_queue = {}

    kafka_settings_queue['kafka_broker_list'] = GeneralConfig.YCL_KAFKA_URL
    kafka_settings_queue['kafka_topic_list'] = topic_name

    # Имя группы потребителей
    kafka_settings_queue['kafka_group_name'] = group_name

    # Формат сообщений
    kafka_format = kafka_settings_queue.get('kafka_format')
    if kafka_format is None:
        kafka_format = 'JSONEachRow'

    kafka_settings_queue['kafka_format'] = kafka_format

    # Пропуск битых (ПРИЧЕСЫВАЕМ НУТРО СООБЩЕНИЙ ДО ПОДАЧИ В ТОПИК КАФКИ)
    kafka_max_block_size = kafka_settings_queue.get('kafka_max_block_size')
    kafka_skip_broken_messages = kafka_settings_queue.get('kafka_skip_broken_messages')

    if kafka_skip_broken_messages:
        if kafka_max_block_size and kafka_skip_broken_messages < kafka_max_block_size:
            kafka_skip_broken_messages = kafka_max_block_size
    else:
        kafka_skip_broken_messages = 65536

    kafka_settings_queue['kafka_skip_broken_messages'] = kafka_skip_broken_messages

    # Причесать настройки
    kafka_row_delimiter = kafka_settings_queue.get('kafka_row_delimiter')
    ycl_repair_settings(settings=kafka_settings_queue)

    if kafka_row_delimiter:
        kafka_settings_queue['kafka_row_delimiter'] = repr(kafka_row_delimiter)

    return kafka_settings_queue


def get_num_partition_consumers(kafka_settings_queue):
    """
    Дабы избежать всяких извращений - строго на строго заклинаем число партиций равное числу потребителей
    При этом если их число не задано - то партиция одна, параллелизм выключен. Не менее строго на строго
    """

    kafka_num_consumers = kafka_settings_queue.get('kafka_num_consumers')
    if kafka_num_consumers is None:
        kafka_num_consumers = 1
        kafka_settings_queue['kafka_num_consumers'] = kafka_num_consumers
        kafka_settings_queue['kafka_thread_per_consumer'] = 0

    return kafka_num_consumers


async def get_ycl_metadata(table_filter=None):
    """
    Получить метаданные таблиц кликхауса
    """
    conn = ycl_get_connection_settings(GeneralConfig.CLICKHOUSE_SHOWCASE_URL)
    res = await ycl.get_metadata(conn=conn, table_filter=table_filter)
    return res


def adaptation_dict_name(client_key, key_name):
    """
    Адаптация разделённого имени словаря к полному
    """

    table_dict = gen_dict_table_name(client_key=client_key, name_dict=key_name)
    return table_dict


def column_repair_for_jinja(column_input):
    """
    Доработка опциональных параметров для ниндзи
    Можно тоже самое сделать средствами ниндзи но её уже сложно читать
    """
    for column_name, column_data in column_input.items():
        if column_data.get('type') is None:
            column_data['type'] = ''

        comment = column_data.get('human_name')
        if not comment:
            comment = column_name

        column_data['comment'] = comment

        if column_data.get('dma') is None:
            column_data['dma'] = ''

        if column_data.get('ttl') is None:
            column_data['ttl'] = ''


def ycl_generate_column_showcase(client_key, column_input, relation_dicts, meta_data):
    """
    Из отношений к словарям - создать колонки типа ALIAS

    ALIAS dictGet(__cl_smpb_test_nsi_alt_organisations, 'OKTMO', tuple(tt1))
    """

    if column_input is None:
        column_input = {}

    if relation_dicts and meta_data:
        # Обязательное условие - наличие полученных метаданных по словарю

        jinja_dict_column = \
            "ALIAS dictGet({{dict_name}}, '{{dict_column}}', " \
            "tuple({% set ns = namespace(first=False)  %}{% for expr in relations %}{% if ns.first %}," \
            "{% else %}{% set ns.first = True %}{% endif%}{{expr}}{% endfor %}))"

        for dict_name_raw, rel_data in relation_dicts.items():

            dict_key = rel_data.get('client_name')
            if not dict_key:
                dict_key = client_key

            dict_name = adaptation_dict_name(client_key=dict_key, key_name=dict_name_raw)
            meta_columns = meta_data.get(dict_name)
            if not meta_columns:
                # Возможно это важно. Но если нет метаданных словаря - считаем что его просто нет
                continue

            # Практически незримая связь, сопоставления порядка первчиных ключей их типам в словаре
            # В кликхаусе это не прослеживается явно. Единственное что удалось придумать - у вас на экране

            type_rel = []
            for rel_column in meta_columns:
                if rel_column['is_pk']:
                    type_rel.append(rel_column['type'])

            relation_keys = rel_data.get('relation_keys')
            if not relation_keys:
                # без ключей нельзя привязаться
                continue

            # Генерируем для каждого ключа оболочку во что он должен быть представлен (функция-преобразователь)
            relations = []
            ind_rel = 0
            for key_rel in relation_keys:
                # СВЯЗЬ С ТИПОМ СЛОВАРЯ! Первый к первому

                expr_rel = ycl_generate_expr_alias(column=key_rel, type=type_rel[ind_rel])
                relations.append(expr_rel)

                ind_rel = ind_rel + 1
                if ind_rel >= len(type_rel):
                    break

            if not relations:
                continue

            columns_list = rel_data.get('columns_list')
            is_ignore_list = rel_data.get('ignore_or_using_list')

            for column in meta_columns:

                if column['is_pk']:
                    # Добавление колонок первичных ключей вызывает ошибку в кликхаусе
                    # Он пишет что не может преобразовать типы... Хоть там и не нужно даже ничего преобразовывать
                    continue

                column_name = column['name']
                column_comment = column.get('comment')
                if not column_comment:
                    column_comment = column_name

                if columns_list:
                    # Список колонок задан. Если не задан то все!
                    if is_ignore_list:
                        # Этот список используется для исключения, тоесть, все кроме тех кто в списке
                        if column_name in is_ignore_list:
                            continue
                    else:
                        # Этот список используется для одобрения, тоесть, только те что в списке
                        if column_name not in is_ignore_list:
                            continue

                alias_expr = jinja_render_str_to_str(
                    str_pattern=jinja_dict_column,
                    render={
                        'dict_name': dict_name,
                        'dict_column': column_name,
                        'relations': relations
                    }
                )

                save_column_name = ycl_name_dict_column_preparation(
                    client_key=client_key,
                    dict_name=dict_name,
                    column_name=column_name
                )

                column_input[save_column_name] = {
                    'type': column['type'],
                    'dma': alias_expr,
                    'ttl': '',
                    'is_dict': True,
                    'comment': column_comment
                }

    return column_input


def ycl_name_dict_column_preparation(client_key, dict_name, column_name):
    """
    Причесать имя колонки (имя словаря_имя колонки)
    """
    proj = get_project_prefix()

    if dict_name.startswith(proj):
        add_column_name = dict_name.replace(proj, '')
        add_column_name = add_column_name.replace(client_key, '')
        if add_column_name.startswith('nsi_'):
            add_column_name = add_column_name[len('nsi_'):]
    else:
        add_column_name = dict_name

    while add_column_name.startswith('_'):
        add_column_name = add_column_name[len('_'):]

    return f'{add_column_name}_{column_name}'


def ycl_generate_expr_alias(column, type):
    """
    Формирование выражения получения словарей
    """

    func = DEFAULT_YCL_PRIM_KEY_FUNC_TYPES.get(type)
    if func:
        res = f'{func}({column})'

    elif type.find('Decimal64') != -1:
        # Переделать в будущем

        func = DEFAULT_YCL_PRIM_KEY_FUNC_TYPES.get('Decimal64')
        scale = 5
        res = f'{func}({column}, {scale})'
    else:
        res = column

    return res


def ycl_repair_settings(settings):
    """
    Кликхаусу явно надо обозначить строку в апострофах
    """

    for key, value in settings.items():
        settings[key] = ycl.modify_settings_preparation_value(value=value)

    return settings


"""
Удаление витрины. Только кликхаус. Кафку не трогаем. 
Она по мифическим причинам просто умирает при удалении топика. 
"""

"""
Удаление словаря 
"""


async def smart_delete_showcases(json_data):
    """
    Удаление Витрин шаг за шагом.

    Шаг 1. Удалить всё в кликхаусе.
    Шаг 2. Сохранить метаданные
    """

    click_house_remove = {}
    meta_data_save = {}

    summary_result = {
        'show_case_input': 0,
        'show_case_success_remove': 0,
        'report_remove': click_house_remove,
        'jsonb_processing': None
    }

    conn_cl = ycl_get_connection_settings(
        GeneralConfig.CLICKHOUSE_SHOWCASE_URL
    )
    none_delete = True
    for client_key, showcase_names in json_data.items():

        summary_result['show_case_input'] = \
            summary_result['show_case_input'] + 1

        report_showcase = {}
        click_house_remove[client_key] = {
            'status': False,
            'msg': 'invalid input data',
            'showcase_input': len(showcase_names),
            'showcase_success_remove': 0,
            'report_showcase': report_showcase
        }
        if len(showcase_names) == 0:
            # очень корявый джсон
            continue

        # Инфа по предыдущему состоянию
        dict_info_old = None
        old_client = await get_client(client_key=client_key)
        if old_client:
            old_info = old_client[0]['structureBody']
            if old_info:
                dict_info_old = old_info.get(DEFAULT_META_NAME_SHOWCASE)

        if not dict_info_old:
            click_house_remove[client_key]['msg'] = 'no active showcases'
            continue

        meta_data_save[client_key] = {DEFAULT_META_NAME_SHOWCASE: dict_info_old}
        click_house_remove[client_key]['msg'] = 'remove bad processing. see report_showcase field'

        for showcase_name in showcase_names:

            result_dict = {
                'showcase_name': showcase_name,
                'ycl_remove': False,
                'msg': 'remove early'
            }

            report_showcase[showcase_name] = result_dict

            # Витрина обязательно должна быть в хранилище метаданных.
            # Просто так удалять всё подряд из кликхауса мы не позволим
            if dict_info_old.get(showcase_name) is None:
                continue

            # А вот теперь как по нотам. Идём в кликхаус - потом сносим файлы.
            # Снести файлы получается всегда - обрабатывать восстановление нет смысла
            try:
                table_showcase_kafka_queue = gen_showcase_table_name_kafka(
                    client_key=client_key, name_showcase=showcase_name
                )

                table_showcase_data = gen_showcase_table_name_showcase(
                    client_key=client_key, name_showcase=showcase_name
                )

                table_showcase_view = gen_showcase_table_name_view(
                    client_key=client_key, name_showcase=showcase_name
                )
                await ycl.delete_table(conn=conn_cl, name=table_showcase_view)
                await ycl.delete_table(conn=conn_cl, name=table_showcase_data)
                await ycl.delete_table(conn=conn_cl, name=table_showcase_kafka_queue)

                result_dict['ycl_remove'] = True
                result_dict['msg'] = 'success remove'

                # Удаляем из старого джсона - да вот так просто. Да в одну строчку решены все проблемы
                dict_info_old.pop(showcase_name)

                click_house_remove[client_key]['showcase_success_remove'] = \
                    click_house_remove[client_key]['showcase_success_remove'] + 1

                # Есть что апдейтнуть
                none_delete = False

            except Exception as exp:
                result_dict['msg'] = str(exp)

        if click_house_remove[client_key]['showcase_input'] == \
                click_house_remove[client_key]['showcase_success_remove']:
            click_house_remove[client_key]['msg'] = 'all showcases well be remove'
            click_house_remove[client_key]['status'] = True

    # Шаг 2 - Если обновлять нечего - ничего не обновляем
    if none_delete:
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
                key_info={DEFAULT_META_NAME_SHOWCASE: None}
            )

            summary_result['show_case_success_remove'] = \
                summary_result['show_case_success_remove'] + \
                click_house_remove[client_key]['showcase_success_remove']

        except Exception as exp:
            report['error_msg'] = str(exp)
            report['status'] = False

        jsonb_processing[client_key] = report

    summary_result['jsonb_processing'] = jsonb_processing
    return summary_result






