"""
КОД НЕ ПОДДЕРЖИВАЕТСЯ. СОХРАНЁН В КАЧЕСТВЕ ПРИМЕРА
Устаревшие методы - создание по одному шаг за шагом
"""

#
# class OldMigr:
#     @staticmethod
#     async def create_or_update_dictionaries(data_json):
#         """
#         Принимает data/models/api/Dictionary_Settings/_post_add_or_update.json
#         Создает или обновляет данные по словарям
#
#         Шаг 1. Файл в ОС - класс питона
#         Шаг 2. Отправка миграции в постгрес
#         Шаг 3. Создание запроса в кликхаус и его отправка
#         Шаг 4. Отправка метаданных
#
#         Если на каком-то из шагов вальнулось - естественно отменить всё необходимо
#         Это капец как сложно - но выполнимо.
#
#         Сначала получим метаданные старые.
#         """
#
#         result_operations = []
#
#         for dict_obj in data_json:
#
#             result_dict = {
#                 'client': None,
#                 'dict': None,
#                 'status': None,
#                 'report_upgrade': None,
#                 'report_downgrade': None,
#                 'msg': '',
#                 'processing_error': '',
#                 'downgrade_error': '',
#             }
#
#             old_info = None
#
#             try:
#                 client_key = dict_obj['client_key']
#                 result_dict['client'] = client_key
#
#                 dict_name = dict_obj['system_name']
#                 result_dict['dict'] = dict_name
#
#                 columns = dict_obj['columns']
#                 dict_comment = dict_obj.get('human_name', dict_name)
#
#                 old_client = await get_client(client_key=client_key)
#                 if old_client:
#                     old_info = old_client[0]['structureBody']
#
#             except Exception as exp:
#
#                 result_dict['status'] = False
#                 result_dict['msg'] = 'invalid processing. invalid input data'
#                 result_dict['processing_error'] = str(exp)
#                 result_operations.append(result_dict)
#
#                 continue
#
#             # Обработать словарь по шагам
#             result_processing = await OldMigr.process_dict_object(
#                 columns=columns,
#                 dict_name=dict_name,
#                 client_key=client_key,
#                 dict_comment=dict_comment,
#                 old_info=old_info
#             )
#
#             result_dict['report_upgrade'] = result_processing
#
#             if result_processing['all_ok']:
#                 # Словарь успешно создан
#
#                 result_dict['status'] = True
#                 result_dict['msg'] = 'success processing'
#             else:
#                 if result_processing['step_counter'] > 0:
#                     # Счётчик шагов больше 0. Значит какая-то операция была выполнена. Но не все. Откатить до отказа
#
#                     if old_info:
#                         # Есть возможность сделать откат - явно сохранена предыдущая версия
#
#                         key_info = {DEFAULT_META_NAME_DICTS: {dict_name: None}}
#
#                         columns = json_recombine_kv_get_info(
#                             key_info=key_info,
#                             source_info=old_info,
#                             final_key=DEFAULT_OLD_INFO_DICT_META_COLUMNS
#                         )
#
#                         result_downgrade = await OldMigr.process_dict_object(
#                             columns=columns,
#                             dict_name=dict_name,
#                             client_key=client_key,
#                             dict_comment=dict_comment,
#                             old_info=old_info,
#                             return_counter=result_processing['step_counter']
#                         )
#
#                         result_dict['report_downgrade'] = result_downgrade
#
#                         if result_processing['all_ok']:
#                             # Откат успешен
#                             result_dict['status'] = False
#                             result_dict['msg'] = 'invalid processing. success downgrade'
#                             result_dict['processing_error'] = result_processing['error_msg']
#                         else:
#                             # Откат неудачен
#                             result_dict['status'] = False
#                             result_dict['msg'] = 'invalid processing. invalid downgrade'
#                             result_dict['processing_error'] = result_processing['error_msg']
#                             result_dict['downgrade_error'] = result_downgrade['error_msg']
#                     else:
#                         # Не сохранена предыдущая версия словаря (первое создание или хранилище недоступно)
#                         result_dict['status'] = False
#                         result_dict['msg'] = 'invalid processing. invalid downgrade'
#                         result_dict['processing_error'] = result_processing['error_msg']
#                         result_dict['downgrade_error'] = 'downgrade is not available'
#                 else:
#                     # Ошибка в парсинге данных - колонки плохие
#                     result_dict['status'] = False
#                     result_dict['msg'] = 'invalid processing. invalid input data'
#                     result_dict['processing_error'] = result_processing['error_msg']
#
#             result_operations.append(result_dict)
#
#         return result_operations
#
#     @staticmethod
#     async def process_dict_object(client_key, columns, dict_name, dict_comment, old_info, return_counter=None):
#         """
#         Обработка словаря шаг за шагом
#
#         Если указан шаг прерывания 'return_counter' - то это откат на предыдущую версию
#         """
#
#         # Шаг 0 - формирование основополагающих данных
#         result_processing = {
#             'all_ok': True,
#             'step_counter': 0,
#             'file_creating': False,
#             'aerich_migration': False,
#             'ycl_req': False,
#             'jsonb_metadata': False,
#             'error_msg': ''
#         }
#
#         try:
#             #   пути
#             model_name = gen_dict_table_name(client_key=client_key, name_dict=dict_name)
#             path_to_models = GeneralConfig.DEFAULT_AERICH_MODEL_APP_PATH / 'NSI'
#             client_folder = path_to_models / model_name
#
#             #   ORM
#             render_orm = create_jinja_dict_for_python(
#                 columns=columns,
#                 model_name=model_name,
#                 dict_comment=dict_comment
#             )
#
#             #   Pydantic
#             render_pydantic = {
#                 'file_import': DEFAULT_MODEL_FILE_NAME,
#                 'class_import': model_name
#             }
#
#             #   YCL dict -> req
#             render_ycl = create_jinja_dict_for_ycl(
#                 columns=columns,
#                 model_name=model_name
#             )
#
#             ycl_req = jinja_render_to_str(src=DEFAULT_JINJA_PATTERN_YCL, render=render_ycl)
#
#             #   JSONB - META
#             dst_info = {
#                 DEFAULT_META_NAME_DICTS: {
#                     dict_name: {
#                         'comment': dict_comment,
#                         'columns': columns
#                     }
#                 }
#             }
#
#             key_info = {DEFAULT_META_NAME_DICTS: {dict_name: None}}
#
#             json_recombine_key_value(
#                 source_info=old_info,
#                 dst_info=dst_info,
#                 key_info=key_info,
#                 final_key=DEFAULT_OLD_INFO_DICT_META_COLUMNS,
#                 final_value=columns
#             )
#
#             # Шаг 1 - Автогенератор классов
#             jinja_render_to_file(
#                 src=DEFAULT_JINJA_PATTERN_ORM,
#                 dst=client_folder / f'{DEFAULT_MODEL_FILE_NAME}.py',
#                 render=render_orm
#             )
#
#             jinja_render_to_file(
#                 src=DEFAULT_JINJA_PATTERN_PYD,
#                 dst=client_folder / f'{DEFAULT_PYDANTIC_FILE_NAME}.py',
#                 render=render_pydantic
#             )
#
#             with open(file=client_folder / '__init__.py', mode='w'):
#                 pass
#
#             result_processing['step_counter'] = result_processing['step_counter'] + 1
#             result_processing['file_creating'] = True
#
#             if return_counter == result_processing['step_counter']:
#                 return result_processing
#
#             # # Шаг 2 Миграция модели через AERICH
#             res = update_tables()
#             if not res:
#                 raise NSIProcessingError(f'Aerich migration error. See {DEFAULT_FILE_MIGRATION_LOG}')
#
#             result_processing['step_counter'] = result_processing['step_counter'] + 1
#             result_processing['aerich_migration'] = True
#
#             if return_counter == result_processing['step_counter']:
#                 return result_processing
#
#             # # Шаг 3 Отправка настроек в кликхаус
#             res = await ycl_create_or_update_dict(sql_dict=ycl_req, model_name=model_name)
#
#             result_processing['step_counter'] = result_processing['step_counter'] + 1
#             result_processing['ycl_req'] = res
#
#             if return_counter == result_processing['step_counter']:
#                 return result_processing
#
#             # Шаг 4 Обновление метаданных
#             res = await create_or_update_client(
#                 client_key=client_key,
#                 add_info=dst_info,
#                 key_info=key_info
#             )
#
#             result_processing['step_counter'] = result_processing['step_counter'] + 1
#             result_processing['jsonb_metadata'] = res
#
#         except Exception as exp:
#             result_processing['all_ok'] = False
#             result_processing['error_msg'] = str(exp)
#
#         return result_processing
#
