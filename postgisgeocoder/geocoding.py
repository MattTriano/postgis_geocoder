from typing import Callable

import geopandas as gpd
import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm

from postgisgeocoder.db import (
    execute_result_returning_query,
    execute_structural_command,
    create_database_schema,
)


def create_user_data_schema(engine: Engine) -> None:
    create_database_schema(engine=engine, schema_name="user_data")


def create_address_table(
    engine: Engine, schema_name: str = "user_data", table_name: str = "address_table"
) -> None:
    execute_structural_command(
        query=f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                full_address varchar(100) PRIMARY KEY 
            );
        """,
        engine=engine,
    )


def add_addr_normalization_columns_to_address_table(
    engine: Engine, schema_name: str = "user_data", table_name: str = "address_table"
) -> None:
    execute_structural_command(
        query=f"""
            ALTER TABLE {schema_name}.{table_name}
                ADD COLUMN IF NOT EXISTS address integer DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS predirabbrev varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS streetname varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS streettypeabbrev varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS postdirabbrev varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS internal varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS location varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS stateabbrev varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS zip varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS parsed boolean DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS zip4 varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS address_alphanumeric varchar DEFAULT NULL;
        """,
        engine=engine,
    )


def setup_address_table(
    engine: Engine, schema_name: str = "user_data", table_name: str = "address_table"
) -> None:
    create_database_schema(engine=engine, schema_name=schema_name)
    create_address_table(engine=engine, schema_name=schema_name, table_name=table_name)
    add_addr_normalization_columns_to_address_table(
        engine=engine, schema_name=schema_name, table_name=table_name
    )


def add_geocode_output_columns_to_address_table(
    engine: Engine, schema_name: str = "user_data", table_name: str = "address_table"
) -> None:
    execute_structural_command(
        query=f"""
            ALTER TABLE {schema_name}.{table_name}
                ADD COLUMN IF NOT EXISTS rating integer DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS norm_address varchar DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS geomout geometry DEFAULT NULL;
        """,
        engine=engine,
    )


def batch_normalize_address_table(
    engine: Engine,
    schema_name: str = "user_data",
    table_name: str = "address_table",
    batch_size: int = 100,
) -> None:
    full_table_name = f"{schema_name}.{table_name}"
    query = f"""
        UPDATE {full_table_name}
        SET
            (
                address, predirabbrev, streetname, streettypeabbrev, postdirabbrev,
                internal, location, stateabbrev, zip, parsed, zip4, address_alphanumeric
            ) = (
                (na).address, (na).predirabbrev, (na).streetname, (na).streettypeabbrev,
                (na).postdirabbrev, (na).internal, (na).location, (na).stateabbrev,
                (na).zip, (na).parsed, (na).zip4, (na).address_alphanumeric
            )
        FROM
            (
                SELECT full_address, streetname
                FROM {full_table_name}
                WHERE streetname IS NULL LIMIT {batch_size}
            ) AS a
            LEFT JOIN LATERAL
            normalize_address(a.full_address) AS na
            ON true
        WHERE a.full_address = {full_table_name}.full_address;
    """
    execute_structural_command(query=query, engine=engine)


def to_sql_on_conflict_do_nothing(table, conn, keys, data_iter) -> None:
    data = [dict(zip(keys, row)) for row in data_iter]
    conn.execute(insert(table.table).on_conflict_do_nothing(), data)


def add_addresses_to_address_table(
    addr_df: pd.DataFrame,
    engine: Engine,
    schema_name: str = "user_data",
    table_name: str = "address_table",
) -> None:
    assert addr_df.shape[1] == 1, "Only one column permitted for addr_df."
    assert addr_df.columns[0] == "full_address", "Only 'full_address' permitted in addr_df."
    addr_df.to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        index=False,
        if_exists="append",
        method=to_sql_on_conflict_do_nothing,
    )


def batch_geocode_address_table(
    engine: Engine,
    schema_name: str = "user_data",
    table_name: str = "address_table",
    batch_size: int = 100,
    rating_threshold: int = 22,
) -> None:
    full_table_name = f"{schema_name}.{table_name}"
    query = f"""
        UPDATE {full_table_name}
        SET 
            (rating, norm_address, geomout) = 
            (COALESCE((g).rating,-1 ), pprint_addy( (g).addy ), (g).geomout)
        FROM (
                SELECT full_address, (address, predirabbrev, streetname, streettypeabbrev, 
                        postdirabbrev, internal, location, stateabbrev, zip, parsed, zip4,
                        address_alphanumeric)::norm_addy AS addy
                FROM {full_table_name}
                WHERE rating IS NULL LIMIT {batch_size}
            ) AS a
            LEFT JOIN LATERAL
            geocode(a.addy) AS g
            ON ((g).rating < {rating_threshold})
        WHERE a.full_address = {full_table_name}.full_address;
    """
    execute_structural_command(query=query, engine=engine)


def get_default_geocode_settings(engine: Engine) -> pd.DataFrame:
    """Returns default geocode_settings."""
    default_geocode_settings = execute_result_returning_query(
        query="SELECT * FROM tiger.geocode_settings;", engine=engine
    )
    return default_geocode_settings


def get_current_geocode_settings(engine: Engine) -> pd.DataFrame:
    """Returns default geocode_settings."""
    geocode_settings = execute_result_returning_query(
        query="SELECT * FROM tiger.geocode_settings;", engine=engine
    )
    return geocode_settings


def count_rows_w_null_values_in_a_column(
    null_check_col: str,
    engine: Engine,
    schema_name: str = "user_data",
    table_name: str = "address_table",
) -> int:
    full_table_name = f"{schema_name}.{table_name}"
    null_rows_df = execute_result_returning_query(
        query=f"""
            SELECT COUNT(*)
            FROM {full_table_name}
            WHERE {null_check_col} IS NULL;
        """,
        engine=engine,
    )
    rows_w_null_col_val_count = null_rows_df["count"].values[0]
    return rows_w_null_col_val_count


def add_addr_standardization_columns_to_an_address_table(
    engine: Engine, schema_name: str = "user_data", table_name: str = "std_address_table"
) -> None:
    execute_structural_command(
        query=f"""
            ALTER TABLE {schema_name}.{table_name}
                ADD COLUMN IF NOT EXISTS building text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS house_num text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS predir text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS qual text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS pretype text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS name text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS suftype text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS sufdir text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS ruralroute text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS extra text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS city text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS state text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS country text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS postcode text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS box text DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS unit text DEFAULT NULL;
        """,
        engine=engine,
    )


def batch_standardize_address_table(
    engine: Engine,
    schema_name: str = "user_data",
    table_name: str = "std_address_table",
    batch_size: int = 100,
) -> None:
    full_table_name = f"{schema_name}.{table_name}"
    query = f"""
        UPDATE {full_table_name}
        SET (
            building, house_num, predir, qual, pretype, name, suftype, sufdir, ruralroute,
            extra, city, state, country, postcode, box, unit
        ) = (
            (sa).building, (sa).house_num, (sa).predir, (sa).qual, (sa).pretype, (sa).name,
            (sa).suftype, (sa).sufdir, (sa).ruralroute, (sa).extra, (sa).city, (sa).state,
            (sa).country, (sa).postcode, (sa).box, (sa).unit
        )
        FROM (
            SELECT full_address, name
            FROM {full_table_name}
            WHERE name IS NULL LIMIT {batch_size}
            ) AS a
        LEFT JOIN LATERAL
        standardize_address(
                'tiger.pagc_lex', 'tiger.pagc_gaz', 'tiger.pagc_rules', a.full_address
            ) AS sa
        ON true
        WHERE a.full_address = {full_table_name}.full_address;
    """
    execute_structural_command(query=query, engine=engine)


def _apply_function_to_all_address_table_rows(
    engine: Engine,
    table_name: str,
    null_check_col: str,
    batch_func: Callable,
    schema_name: str = "user_data",
    batch_size: int = 100,
) -> None:
    rows_left = count_rows_w_null_values_in_a_column(
        engine=engine, null_check_col=null_check_col, schema_name=schema_name, table_name=table_name
    )
    batches_left = (rows_left // batch_size) + (rows_left % batch_size > 0)
    for i in tqdm(range(batches_left)):
        batch_func(
            engine=engine, schema_name=schema_name, table_name=table_name, batch_size=batch_size
        )


def normalize_all_addresses_in_address_table(engine: Engine, batch_size: int = 100) -> None:
    _apply_function_to_all_address_table_rows(
        engine=engine,
        schema_name="user_data",
        table_name="address_table",
        null_check_col="streetname",
        batch_func=batch_normalize_address_table,
        batch_size=batch_size,
    )


def standardize_all_addresses_in_address_table(engine: Engine, batch_size: int = 100) -> None:
    _apply_function_to_all_address_table_rows(
        engine=engine,
        schema_name="user_data",
        table_name="std_address_table",
        null_check_col="name",
        batch_func=batch_standardize_address_table,
        batch_size=batch_size,
    )
