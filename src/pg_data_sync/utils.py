import os
import yaml
import shutil
from datetime import datetime
from uuid import uuid4
from typing import Dict
from pathlib import Path
from .models import Config


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


def get_download_path() -> str:
    download_path = Path(get_app_files_dir()).joinpath(
        'download', str(uuid4()))

    return str(download_path)


def get_tmp_db_name() -> str:
    id = str(uuid4()).replace('-', '')

    return f'tmp_{id}'


def get_backup_db_name(db_name: str) -> str:
    return f'{db_name}_bak_{datetime.now().strftime('%Y%m%d%H%M%S')}'


def delete_file_or_dir(path: str) -> None:
    path_obj = Path(path)

    if not path_obj.exists():
        return

    try:
        if path_obj.is_dir():
            shutil.rmtree(path)
        else:
            path_obj.unlink()
    except Exception as err:
        raise Exception(f'Error deleting "{path}": {err}')


def get_file_size(content_length: str | None) -> float:
    if not content_length:
        return 0

    return int(content_length) / (1024 * 1024)


__all__ = ['get_env', 'load_config', 'get_download_path', 'get_tmp_db_name',
           'get_backup_db_name', 'delete_file_or_dir', 'get_file_size']
