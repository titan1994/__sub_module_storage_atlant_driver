"""
Инициализация всех подмодулей проекта
Предварительно в консоли надо авторизоваться токеном!
"""

from subprocess import check_output, CalledProcessError, STDOUT
from pathlib import Path
from shutil import rmtree
from os import makedirs

INIT_TRUE_OR_DELETE_FALSE = True

ACCOUNT_NAME = 'titan1994'
GEN_MOD_FOLDER = 'MODS'

submodules = {
    '__sub_module_scripts': 'scripts',
    '__sub_module_documents': 'documents',
    '__sub_module_drivers': 'DRIVERS',
    '__sub_modile_name_space_standart': 'standart_namespace',
    '__sub_module_fast_api': 'rest_core',
}

file_append = ['.gitignore', '__init__.py']
dirs_clean = ['.idea']


def run(cmd):
    """
    Команды операционной системы
    :return:
    """

    try:
        data_cmd = check_output(cmd, shell=True, universal_newlines=True, stderr=STDOUT)
        status_cmd = 0

    except CalledProcessError as ex:

        data_cmd = ex.output
        status_cmd = ex.returncode

    if data_cmd[-1:] == '\n':
        data_cmd = data_cmd[:-1]

    data_cmd = data_cmd.encode(encoding='1251', errors='ignore')
    data_cmd = data_cmd.decode(encoding='IBM866', errors='ignore')

    return status_cmd, data_cmd


def run_with_info(cmd):
    print(cmd)
    status_cmd, data_cmd = run(cmd=cmd)
    print('state:', status_cmd)
    print('info:', data_cmd)


if INIT_TRUE_OR_DELETE_FALSE:
    # ДОБАВЛЕНИЕ ПОДМОДУЛЕЙ

    # Добавляем файлы-пустышки в MODS
    try:
        makedirs(GEN_MOD_FOLDER)
    except Exception as exp:
        print(exp)

    for file_ap in file_append:
        try:
            with open(Path(GEN_MOD_FOLDER) / file_ap, 'w'):
                pass
        except Exception as exp:
            print(exp)

    # Подключаем модули
    for sub_module, mpath in submodules.items():
        run_with_info(f'git submodule add -f https://github.com/{ACCOUNT_NAME}/{sub_module} {GEN_MOD_FOLDER}/{mpath}')

        path_folder_module = Path(GEN_MOD_FOLDER) / mpath
        for dir_cl in dirs_clean:
            try:
                rmtree(Path(GEN_MOD_FOLDER) / mpath / dir_cl)
            except Exception as exp:
                print(exp)

    # Первичная инициализация
    run_with_info('git submodule init')

    # Переполучение данных - переключение в ветку Мастер
    run_with_info('git submodule foreach --recursive git checkout master')
    run_with_info('git submodule foreach --recursive git pull')
else:
    # Удаляем модули
    for _, mpath in submodules.items():
        run_with_info(f'git submodule deinit -f -- {GEN_MOD_FOLDER}/{mpath}')
        run_with_info(f'git rm -f {GEN_MOD_FOLDER}/{mpath}')
        run_with_info(f'git rm -r --cached {GEN_MOD_FOLDER}/{mpath}')
        try:
            rmtree(Path(GEN_MOD_FOLDER) / mpath)
        except Exception as exp:
            print(exp)

        try:
            rmtree(Path('.git') / 'modules' / GEN_MOD_FOLDER / mpath)
        except Exception as exp:
            print(exp)

    # Удаляем директорию модулей
    # try:
    #     rmtree(Path(GEN_MOD_FOLDER))
    # except Exception as exp:
    #     print(exp)
