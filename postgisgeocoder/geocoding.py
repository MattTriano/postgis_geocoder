import geopandas as gpd
import pandas as pd
from sqlalchemy.engine.base import Engine
from sqlalchemy.dialects.postgresql import insert

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
                ADD COLUMN IF NOT EXISTS predirabbrev varchar(5) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS streetname varchar(50) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS streettypeabbrev varchar(10) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS postdirabbrev varchar(5) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS internal varchar(20) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS location varchar(50) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS stateabbrev varchar(2) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS zip varchar(5) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS parsed boolean DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS zip4 varchar(4) DEFAULT NULL,
                ADD COLUMN IF NOT EXISTS address_alphanumeric varchar(10) DEFAULT NULL;
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
