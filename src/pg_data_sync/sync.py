import time
import traceback
from datetime import date
from uuid import uuid4
from pathlib import Path
from typing import List
from . import dataset, db, utils
from .models import DatasetConfig, IndexingConfig, FileMap, Format

tmp_dbs_created: List[str] = []
roles_created: List[str] = []


async def start() -> int:
    download_path: str = ''

    try:
        start = time.time()
        config = utils.load_config()
        dataset_updated = await dataset.get_dataset_update_date(config.dataset.metadata_id, config.dataset.area_code, config.dataset.area_type, config.dataset.epsg, config.dataset.format)
        file_maps = [file_map for file_map in config.dataset.files if await _should_restore_db(file_map.db_name, dataset_updated)]

        if not file_maps:
            print('No need to restore database(s). Aborting...')
            return -2

        download_path = await _download_dataset(config.dataset)

        for file_map in file_maps:
            await _restore_database(download_path, file_map, config.dataset.format, config.indexing)

        utils.delete_file_or_dir(download_path)

        print(f'Job finished in {round(time.time() - start, 2)} sec.')
        return 0
    except Exception:
        err = traceback.format_exc()
        print(err)
        await _clean_up(download_path)
        return -1


async def _download_dataset(config: DatasetConfig) -> str:
    download_url = await dataset.place_order(config)
    download_path = utils.get_download_path()
    download_filename = str(Path(download_path).joinpath(f'{uuid4()}.zip'))

    await dataset.download_file(download_url, download_filename)
    dataset.extract_archive(download_filename, download_path)

    return download_path


async def _restore_database(download_path: str, file_map: FileMap, format: Format, indexing_configs: List[IndexingConfig] | None) -> None:
    resource_path = dataset.get_resource_path(
        download_path, file_map.glob)
    tmp_db_name = utils.get_tmp_db_name()

    if file_map.db_role and not await db.role_exists(file_map.db_role):
        await db.create_role(file_map.db_role, file_map.db_role_pwd or utils.get_env('PGPASSWORD'))
        roles_created.append(file_map.db_role)

    await db.create_db(tmp_db_name)
    tmp_dbs_created.append(tmp_db_name)

    await db.create_extension(tmp_db_name, 'postgis')

    if format == Format.FGDB:
        schema = file_map.db_schema or file_map.db_name

        if schema != 'public':
            await db.create_schema(tmp_db_name, schema)

        db.filegdb_to_postgis(resource_path, tmp_db_name, schema)
    else:
        db.restore_database(resource_path, tmp_db_name)
        await db.rename_schemas(tmp_db_name, file_map.db_schema or file_map.db_name)

    await db.create_indexes(file_map.db_name, tmp_db_name, indexing_configs)

    await db.set_db_comment(tmp_db_name)

    await db.close_active_connections(file_map.db_name)

    if await db.db_exists(file_map.db_name):
        await db.rename_db(file_map.db_name, utils.get_backup_db_name(file_map.db_name))

    await db.close_active_connections(tmp_db_name)
    await db.rename_db(tmp_db_name, file_map.db_name)


async def _should_restore_db(db_name: str, dataset_updated: date | None) -> bool:
    if not await db.db_exists(db_name):
        return True

    db_created = await db.get_db_creation_date(db_name)

    return dataset_updated >= db_created if db_created and dataset_updated else True


async def _clean_up(download_path: str) -> None:
    print('Cleaning up...')

    if download_path:
        utils.delete_file_or_dir(download_path)

    for db_name in tmp_dbs_created:
        await db.delete_db(db_name)

    for role_name in roles_created:
        await db.delete_role(role_name)

__all__ = ['start']
