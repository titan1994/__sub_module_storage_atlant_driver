"""
Индивидуальный скрипт очистки проекта
"""

from shutil import rmtree
from distutils.dir_util import copy_tree
from pathlib import Path

PROJECT_GENERAL_FOLDER = Path(__file__).parent


def del_all_file_in_path(file, add_file=None):
    """
    Очистка каталога
    """

    catalog = Path(file)

    if not catalog.exists():
        print('NOT EXISTS:', catalog)
        return False

    if not catalog.is_dir():
        print('NOT DIR:', catalog)
        return False

    files = catalog.glob("*")
    for file_del in files:
        del_file_from_path(file_del)

    if isinstance(add_file, str):
        file_add_list = [add_file]

    elif isinstance(add_file, list):
        file_add_list = add_file
    else:
        file_add_list = []

    for file_create in file_add_list:
        with open(catalog / file_create, 'w') as fobj:
            print(f'{file_create} ADD:', catalog)

    return True


def del_file_from_path(file):
    """
    Удалить файл/каталог единично
    """
    try:
        if file.is_file():
            file.unlink()
        else:
            rmtree(file)
    except Exception as exp:
        print(exp)


def copy_file_from_path(src, dst):
    """
    Копировать файлы
    """
    try:
        if isinstance(src, Path):
            src_obj = str(src.absolute())
        else:
            src_obj = src

        if isinstance(dst, Path):
            dst_obj = str(dst.absolute())
        else:
            dst_obj = dst

        copy_tree(src=src_obj, dst=dst_obj)
    except Exception as exp:
        print(exp)


if __name__ == '__main__':
    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__docker' / 'volumes' / 'clickhouse' / 'cl_db', '.gitkeep')
    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__docker' / 'volumes' / 'kafka' / 'k1', '.gitkeep')
    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__docker' / 'volumes' / 'kafka' / 'k2', '.gitkeep')
    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__docker' / 'volumes' / 'kafka' / 'k3', '.gitkeep')
    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__docker' / 'volumes' / 'kafka' / 'zoo' / 'data', '.gitkeep')
    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__docker' / 'volumes' / 'kafka' / 'zoo' / 'log', '.gitkeep')

    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__jsonb_docker_migrations' / 'aerich', '.gitkeep')
    del_file_from_path(PROJECT_GENERAL_FOLDER / '__jsonb_docker_migrations' / 'migration.log')

    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__migrations' / 'aerich', '.gitkeep')
    del_file_from_path(PROJECT_GENERAL_FOLDER / '__migrations' / 'migration.log')

    del_all_file_in_path(PROJECT_GENERAL_FOLDER / '__fast_api_app' / 'models' / 'general' / 'NSI', '__init__.py')

    del_all_file_in_path(PROJECT_GENERAL_FOLDER / 'data_launch_system' / 'dict_load_xlsx' / 'a_loads', '.gitkeep')
    del_all_file_in_path(PROJECT_GENERAL_FOLDER / 'data_launch_system' / 'dict_load_xlsx' / 'z_complete', '.gitkeep')
    # copy_file_from_path(
    #     src=PROJECT_GENERAL_FOLDER/'data_launch_system'/'dict_load_xlsx'/'z_example',
    #     dst=PROJECT_GENERAL_FOLDER/'data_launch_system'/'dict_load_xlsx'/'a_loads',
    # )

    del_all_file_in_path(PROJECT_GENERAL_FOLDER / 'data_launch_system' / 'first_launch' / 'init_file_a_loads',
                         '.gitkeep')
    del_all_file_in_path(PROJECT_GENERAL_FOLDER / 'data_launch_system' / 'first_launch' / 'init_file_z_ok', '.gitkeep')
    # copy_file_from_path(
    #     src=PROJECT_GENERAL_FOLDER/'data_launch_system'/'first_launch'/'z_example',
    #     dst=PROJECT_GENERAL_FOLDER/'data_launch_system'/'first_launch'/'init_file_a_loads',
    # )
