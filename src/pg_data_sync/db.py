import os
import subprocess
import time
from collections import Counter, defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.sql import SQL, Composed, Identifier, Literal, Placeholder
from .models import IndexingConfig


async def get_connection(db_name: str) -> AsyncConnection:
    db_user = os.getenv('PGUSER') or 'postgres'

    return await AsyncConnection.connect(f'dbname={db_name} user={db_user}')


async def create_db(db_name: str) -> None:
    sql = SQL('CREATE DATABASE {0}').format(Identifier(db_name))

    try:
        async with await get_connection('postgres') as conn:
            await conn.set_autocommit(True)

            async with conn.cursor() as cur:
                await cur.execute(sql)

        print(f'Database created: {db_name}')
    except Exception as err:
        raise Exception(f'Error creating database: {err}')


async def create_extension(db_name: str, extension: str) -> None:
    sql = SQL('CREATE EXTENSION IF NOT EXISTS {0}').format(
        Identifier(extension))

    try:
        async with await get_connection(db_name) as conn:
            await conn.set_autocommit(True)

            async with conn.cursor() as cur:
                await cur.execute(sql)

        print(f'Extension created: {extension}')
    except Exception as err:
        raise Exception(f'Error creating extension: {err}')


async def create_schema(db_name: str, schema: str) -> None:
    sql = SQL('CREATE SCHEMA {0}').format(Identifier(schema))

    try:
        async with await get_connection(db_name) as conn:
            await conn.set_autocommit(True)

            async with conn.cursor() as cur:
                await cur.execute(sql)

        print(f'Schema created: {schema}')
    except Exception as err:
        raise Exception(f'Error creating schema: {err}')


async def create_role(role_name: str, db_password: str) -> None:
    sql = SQL("CREATE ROLE {0} WITH LOGIN PASSWORD {1}").format(
        Identifier(role_name), Literal(db_password))

    try:
        async with await get_connection('postgres') as conn:
            await conn.set_autocommit(True)

            async with conn.cursor() as cur:
                await cur.execute(sql)

        print(f'Role created: {role_name}')
    except Exception as err:
        raise Exception(f'Error creating role: {err}')


def restore_database(filepath: str, db_name: str) -> None:
    print('Restoring database...')

    path = Path(filepath)

    if path.suffix == '.sql':
        command = [
            '/usr/bin/psql',
            '-U',
            'postgres',
            '-d',
            db_name,
            '-f',
            filepath
        ]
    elif path.suffix == '.backup':
        command = [
            '/usr/bin/pg_restore',
            '-U',
            'postgres',
            '-d',
            db_name,
            filepath
        ]
    else:
        raise Exception(f'Invalid database dump type "{path.name}"')

    start = time.time()

    try:
        subprocess.run(
            command, capture_output=True, text=True, check=False)

        print(f'Database restored in {round(time.time() - start, 2)} sec.')
    except Exception as err:
        raise Exception(f'Error restoring database: {err}')


def filegdb_to_postgis(filepath: str, db_name: str, schema: str) -> None:
    print('Converting FGDB to PostGIS database...')

    db_host = os.environ.get('PGHOST')
    db_password = os.environ.get('PGPASSWORD')

    command = [
        '/usr/bin/ogr2ogr',
        '-f',
        'PostgreSQL',
        f'PG:host={db_host} port=5432 dbname={db_name} user=postgres password={db_password}',
        filepath,
        '-lco',
        f'SCHEMA={schema}',
        '-lco',
        'GEOMETRY_NAME=shape',
        '-lco',
        'FID=objectid',
        '--config',
        'OGR_ORGANIZE_POLYGONS',
        'ONLY_CCW',
        '-nlt',
        'CONVERT_TO_LINEAR',
        '-overwrite'
    ]

    start = time.time()

    try:
        result = subprocess.run(
            command, capture_output=True, text=True, check=True)

        result.check_returncode()

        print(
            f'FGDB converted to PostGIS database in {round(time.time() - start, 2)} sec.')
    except Exception as err:
        raise Exception(f'Error converting FGDB to PostGIS database: {err}')


async def close_active_connections(db_name: str) -> None:
    pids = await get_active_connections(db_name)

    if not pids:
        return

    statements = [SQL('SELECT pg_terminate_backend({0})').format(
        pid) for pid in pids]

    count = 0

    async with await get_connection('postgres') as conn:
        for statement in statements:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(statement)
                    count += 1
            except Exception as err:
                print(f'Error closing active connection: {err}')

    if count:
        print(f'{count} active connection(s) closed')


async def rename_db(old_db_name: str, new_db_name: str) -> None:
    sql = SQL('ALTER DATABASE {0} RENAME TO {1}').format(
        Identifier(old_db_name), Identifier(new_db_name))

    try:
        async with await get_connection('postgres') as conn:
            await conn.set_autocommit(True)

            async with conn.cursor() as cur:
                await cur.execute(sql)

        print(f'Database renamed from {old_db_name} to {new_db_name}')
    except Exception as err:
        raise Exception(f'Error renaming database: {err}')


async def rename_schemas(db_name: str, prefix: str) -> None:
    schema_names = await get_schema_names(db_name, prefix)

    if not schema_names:
        return

    statements: List[Composed] = []

    for name in schema_names:
        index = name.rfind('_')
        new_name = name[0:index]
        statements.append(
            SQL('ALTER SCHEMA {0} RENAME TO {1}').format(Identifier(name), Identifier(new_name)))

    try:
        async with await get_connection(db_name) as conn:
            await conn.set_autocommit(True)

            async with conn.cursor() as cur:
                for statement in statements:
                    await cur.execute(statement)

        print('Schema(s) renamed')
    except Exception as err:
        raise Exception(f'Error renaming schema(s): {err}')


async def delete_db(db_name: str) -> None:
    sql = SQL('DROP DATABASE IF EXISTS {0} WITH (FORCE)').format(
        Identifier(db_name))

    try:
        async with await get_connection('postgres') as conn:
            await conn.set_autocommit(True)

            async with conn.cursor() as cur:
                await cur.execute(sql)

        print(f'Database deleted: {db_name}')
    except Exception as err:
        raise Exception(f'Error deleting old database: {err}')


async def delete_role(role_name: str) -> None:
    sql = SQL('DROP ROLE IF EXISTS {0}').format(
        Identifier(role_name))

    try:
        async with await get_connection('postgres') as conn:
            await conn.set_autocommit(True)

            async with conn.cursor() as cur:
                await cur.execute(sql)

        print(f'Role deleted: {role_name}')
    except Exception as err:
        raise Exception(f'Error deleting role: {err}')


async def set_db_comment(db_name: str) -> None:
    date_str = datetime.now().strftime('%d.%m.%Y')
    comment = f'Version: {date_str}'

    sql = SQL("COMMENT ON DATABASE {0} IS {1}").format(
        Identifier(db_name), Literal(comment))

    try:
        async with await get_connection('postgres') as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)

        print('Database version date set')
    except Exception as err:
        raise Exception(f'Error setting database comment: {err}')


async def get_db_creation_date(db_name: str) -> date | None:
    sql = SQL("""
        SELECT (pg_stat_file('base/'||oid||'/PG_VERSION')).modification 
        FROM pg_database 
        WHERE datname = {0}
    """).format(Literal(db_name))

    try:
        async with await get_connection('postgres') as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                result = await cur.fetchone()

                if not result:
                    return None

                mod_date: datetime = result[0]

                return mod_date.date()
    except Exception as err:
        raise Exception(f'Error getting database creation date: {err}')


async def db_exists(db_name: str) -> bool:
    sql = SQL("SELECT 1 FROM pg_database WHERE datname = {0}").format(
        Literal(db_name))

    try:
        async with await get_connection('postgres') as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                return await cur.fetchone() != None
    except Exception as err:
        raise Exception(f'Error checking database existence: {err}')


async def create_indexes(db_name: str, tmp_db_name: str, configs: List[IndexingConfig] | None) -> None:
    if not configs:
        return

    print('Creating indexes...')

    created = 0
    start = time.time()

    for config in configs:
        if not db_name in config.dbs:
            continue

        indexes = await _get_indexes(tmp_db_name, config.schemas)

        for schema_name in config.schemas:
            geom_columns = await _get_all_geom_columns(tmp_db_name, schema_name, config.tables) if config.geom_index else {}

            for table_name in config.tables:
                if config.id_column and not _has_primary_key(indexes, schema_name, table_name):
                    await create_primary_key(tmp_db_name, schema_name, table_name, config.id_column)
                    created += 1

                if geom_columns:
                    geom_column_names = geom_columns.get(table_name, [])

                    for column_name in geom_column_names:
                        if not _has_geom_index(indexes, schema_name, table_name, column_name):
                            await create_geom_index(tmp_db_name, schema_name, table_name, column_name)
                            created += 1

                col_indexes = config.indexes or []

                for column_names in col_indexes:
                    if not _has_index(indexes, schema_name, table_name, column_names):
                        await create_index(tmp_db_name, schema_name, table_name, column_names)
                        created += 1

    print(f'{created} indexes created in {round(time.time() - start, 2)} sec.')


async def view_exists(db_name: str, schema_name: str, view_name: str) -> bool:
    sql = SQL("""
        SELECT 1
        FROM pg_views
        WHERE schemaname = {0} 
            AND viewname = {1}
    """).format(Literal(schema_name), Literal(view_name))

    try:
        async with await get_connection(db_name) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                return await cur.fetchone() != None
    except Exception as err:
        raise Exception(f'Error checking view existence: {err}')


async def role_exists(role_name: str) -> bool:
    sql = SQL("""
        SELECT 1 
        FROM pg_roles 
        WHERE rolname = {0}
    """).format(Literal(role_name))

    try:
        async with await get_connection('postgres') as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                return await cur.fetchone() != None
    except Exception as err:
        raise Exception(f'Error checking role existence: {err}')


async def get_active_connections(db_name: str) -> List[int]:
    sql = SQL("SELECT pid FROM pg_stat_activity WHERE datname = {0} AND backend_type != 'autovacuum worker'").format(
        Literal(db_name))

    try:
        async with await get_connection('postgres') as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                return [record[0] async for record in cur]
    except Exception as err:
        raise Exception(f'Error getting active connections: {err}')


async def get_schema_names(db_name: str, prefix: str) -> List[str]:
    sql = SQL("SELECT nspname FROM pg_namespace WHERE nspname LIKE {0}").format(
        Literal(prefix + '%'))

    try:
        async with await get_connection(db_name) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                return [record[0] async for record in cur]
    except Exception as err:
        raise Exception(f'Error getting schema names: {err}')


async def create_primary_key(db_name: str, schema_name: str, table_name: str, column_name: str) -> None:
    sql1 = SQL("""
        ALTER TABLE {0}.{1}
        ALTER COLUMN {2}
        SET NOT NULL
    """).format(
        Identifier(schema_name),
        Identifier(table_name),
        Identifier(column_name)
    )

    sql2 = SQL("""
        ALTER TABLE {0}.{1}
        ALTER COLUMN {2} ADD GENERATED BY DEFAULT AS IDENTITY
    """).format(
        Identifier(schema_name),
        Identifier(table_name),
        Identifier(column_name)
    )

    sql3 = SQL("""
        ALTER TABLE {0}.{1}
        ADD PRIMARY KEY ({2})
    """).format(
        Identifier(schema_name),
        Identifier(table_name),
        Identifier(column_name)
    )

    try:
        async with await get_connection(db_name) as conn:
            await conn.set_autocommit(False)

            async with conn.cursor() as cur:
                await cur.execute(sql1)
                await cur.execute(sql2)
                await cur.execute(sql3)

            await conn.commit()
    except Exception as err:
        raise Exception(
            f'Error creating primary key on column {column_name} in table {schema_name}.{table_name}', err)


async def create_index(db_name: str, schema_name: str, table_name: str, column_names: List[str]) -> None:
    index_name = f'{table_name}_{"_".join(column_names)}_idx'

    sql = SQL('CREATE INDEX {0} ON {1}.{2} ({3})').format(
        Identifier(index_name),
        Identifier(schema_name),
        Identifier(table_name),
        SQL(', ').join(Identifier(col_name) for col_name in column_names)
    )

    try:
        async with await get_connection(db_name) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
    except Exception as err:
        raise Exception(
            f'Error creating index on column(s) {", ".join(column_names)} in table {schema_name}.{table_name}', err)


async def create_geom_index(db_name: str, schema_name: str, table_name: str, column_name: str) -> None:
    index_name = f'{table_name}_{column_name}_geom_idx'

    sql = SQL('CREATE INDEX {0} ON {1}.{2} USING GIST ({3})').format(
        Identifier(index_name),
        Identifier(schema_name),
        Identifier(table_name),
        Identifier(column_name)
    )

    try:
        async with await get_connection(db_name) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
    except Exception as err:
        raise Exception(
            f'Error creating geometry index on column {column_name} in table {schema_name}.{table_name}', err)


async def get_columns(db_name: str, schema_name: str, table_name: str) -> List[str]:
    sql = SQL("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = {0}
            AND table_name = {1}
        ORDER BY ordinal_position
    """).format(Literal(schema_name), Literal(table_name))

    try:
        async with await get_connection(db_name) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                return [record[0] async for record in cur]

    except Exception as err:
        raise Exception(f'Error getting column names: {err}')


async def get_geom_columns(db_name, schema_name, table_name) -> List[str]:
    sql = SQL("""
        SELECT f_geometry_column
        FROM geometry_columns
        WHERE f_table_schema = {0}
            AND f_table_name = {1}
    """).format(Literal(schema_name), Literal(table_name))

    try:
        async with await get_connection(db_name) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                return [record[0] async for record in cur]

    except Exception as err:
        raise Exception(f'Error getting geometry column names: {err}')


async def _get_all_geom_columns(db_name: str, schema_name: str, table_names: List[str]) -> Dict[str, List[str]]:
    sql = SQL("""
        SELECT f_table_name, f_geometry_column
        FROM geometry_columns 
        WHERE f_table_schema = {0} AND f_table_name IN ({1})
    """).format(
        Literal(schema_name), SQL(', ').join(Placeholder() * len(table_names))
    )

    try:
        async with await get_connection(db_name) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, table_names)
                result = [record async for record in cur]
                grouped = defaultdict(list)

                for key, value in result:
                    grouped[key].append(value)

                return dict(grouped)
    except Exception as err:
        raise Exception(f'Error getting geometry column names: {err}')


async def _get_indexes(db_name: str, schema_names: List[str]) -> List[Dict[str, Any]]:
    sql = SQL("""
        SELECT
          n.nspname AS schema_name,
          t.relname AS table_name,
          am.amname AS index_type,
          ix.indisunique AS is_unique,
          ix.indisprimary AS is_primary,
          array_agg(
            a.attname
            ORDER BY
            x.ord
          ) AS indexed_columns
        FROM
          pg_class t
          JOIN pg_namespace n ON n.oid = t.relnamespace
          JOIN pg_index ix ON t.oid = ix.indrelid
          JOIN pg_class i ON i.oid = ix.indexrelid
          JOIN pg_am am ON am.oid = i.relam
          JOIN unnest(ix.indkey) WITH ORDINALITY AS x (attnum, ord) ON TRUE
          LEFT JOIN pg_attribute a ON a.attrelid = t.oid
          AND a.attnum = x.attnum
        WHERE
          t.relkind = 'r'
          AND n.nspname IN ({0})
        GROUP BY
          n.nspname,
          t.relname,
          i.relname,
          am.amname,
          ix.indisunique,
          ix.indisprimary
        ORDER BY
          schema_name, 
          table_name              
    """).format(
        SQL(', ').join(Placeholder() * len(schema_names))
    )

    async with await get_connection(db_name) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, schema_names)
            return [record async for record in cur]


def _has_primary_key(indexes: List[Dict[str, Any]], schema_name: str, table_name: str) -> bool:
    return any(index['schema_name'] == schema_name and index['table_name'] ==
               table_name and index['is_primary'] == True for index in indexes)


def _has_geom_index(indexes: List[Dict[str, Any]], schema_name: str, table_name: str, column_name: str) -> bool:
    return any(index['schema_name'] == schema_name and index['table_name'] ==
               table_name and index['index_type'] == 'gist' and index['indexed_columns'][0] ==
               column_name for index in indexes)


def _has_index(indexes: List[Dict[str, Any]], schema_name: str, table_name: str, column_names: List[str]) -> bool:
    return any(index['schema_name'] == schema_name and index['table_name'] ==
               table_name and Counter(index['indexed_columns']) == Counter(column_names) for index in indexes)


__all__ = ['close_active_connections', 'create_db', 'create_extension', 'create_geom_index', 'create_index', 'create_indexes',
           'create_primary_key', 'create_role', 'create_schema', 'db_exists', 'delete_db', 'dict_row', 'filegdb_to_postgis',
           'get_active_connections', 'get_columns', 'get_connection', 'get_db_creation_date', 'get_geom_columns', 'get_schema_names',
           'rename_db', 'rename_schemas', 'restore_database', 'role_exists', 'set_db_comment', 'view_exists']
