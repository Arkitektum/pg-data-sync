import os
import yaml
import shutil
from datetime import datetime
from uuid import uuid4
from typing import Dict, List
from pathlib import Path
from .models import Config, IndexConfig


def get_env(key: str) -> str:
    if key in os.environ:
        return os.environ[key]

    raise Exception(f'Environment variable "{key}" is not set. Aborting...')


def get_app_files_dir() -> str:
    return get_env('APP_FILES_DIR')


def load_config() -> Config:
    file_path = Path(get_app_files_dir()).joinpath('config.yml')

    if not file_path.exists():
        raise Exception(f'Configuration file "{file_path}" not found')

    with open(file_path) as file:
        result: Dict = yaml.safe_load(file)

    return Config(**result)


def load_index_configs() -> List[IndexConfig]:
    file_path = Path(get_app_files_dir()).joinpath('indexing.yml')
    configs: List[IndexConfig] = []

    if not file_path.exists():
        return configs

    with open(file_path) as file:
        result = yaml.safe_load_all(file)
        config: Dict
        
        for config in result:
            configs.append(IndexConfig(**config))

    return configs


def get_download_path() -> str:
    download_path = Path(get_app_files_dir()).joinpath(
        'download', str(uuid4()))

    return str(download_path)


def get_db_tmp_name() -> str:
    id = str(uuid4()).replace('-', '')

    return f'tmp_{id}'


def get_db_backup_name(db_name: str) -> str:
    return f'{db_name}_bak_{datetime.now().strftime('%Y%m%d%H%M%S')}'


def delete_file_or_dir(path: str) -> None:
    path_obj = Path(path)

    try:
        if path_obj.is_dir():
            shutil.rmtree(path)
        else:
            path_obj.unlink()
    except Exception as err:
        raise Exception(f'Error deleting "{path}": {err}')


__all__ = ['get_env', 'load_config', 'get_download_path',
           'get_db_tmp_name', 'get_db_backup_name', 'delete_file_or_dir']
