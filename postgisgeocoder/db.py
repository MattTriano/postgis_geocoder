import os
from typing import Dict, List, Union
import yaml

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine
from sqlalchemy.schema import CreateSchema

from postgisgeocoder.utils import get_project_root_dir


def get_connection_url_from_secrets() -> URL:
    secret_dir = os.path.join(get_project_root_dir(), "secrets")

    with open(os.path.join(secret_dir, "postgresql_password.txt"), "r") as sf:
        password = sf.read()
    with open(os.path.join(secret_dir, "postgresql_user.txt"), "r") as sf:
        username = sf.read()
    with open(os.path.join(secret_dir, "postgresql_db.txt"), "r") as sf:
        db_name = sf.read()

    return URL.create(
        drivername="postgresql+psycopg2",
        host="localhost",
        username=username,
        database=db_name,
        password=password,
        port=4326,
    )


def get_engine_from_secrets(use_sqlalchemy_v2: bool = False, echo: bool = False) -> Engine:
    connection_url = get_connection_url_from_secrets()
    engine = create_engine(connection_url, future=use_sqlalchemy_v2, echo=echo)
    return engine


# def get_db_connection_from_credential_file(
#     credential_path: os.path,
# ) -> pg.extensions.connection:
#     with open(credential_path) as cred_file:
#         credentials = yaml.load(cred_file, Loader=yaml.FullLoader)

#     conn = pg.connect(
#         dbname=credentials["database"],
#         user=credentials["username"],
#         password=credentials["password"],
#         port=credentials["port"],
#         host=credentials["host"],
#     )
#     conn.set_session(autocommit=True)
#     return conn


# def get_db_connection_url_from_credential_file(
#     credential_path: os.path,
# ) -> URL:
#     """Returns a connection string with the permissions corresponding to
#     the credentials in the file at credential_path."""
#     with open(credential_path) as cred_file:
#         credentials = yaml.load(cred_file, Loader=yaml.FullLoader)

#     return URL.create(
#         drivername=credentials["driver"],
#         host=credentials["host"],
#         username=credentials["username"],
#         database=credentials["database"],
#         password=credentials["password"],
#         port=credentials["port"],
#     )


# def get_engine_from_credential_file(credential_path: str) -> Engine:
#     """Returns an sqlalchemy engine with the permissions corresponding to
#     the credentials in the file at credential_path."""
#     connection_url = get_db_connection_url_from_credential_file(credential_path)
#     return create_engine(connection_url)


def execute_result_returning_query(query: str, engine: Engine) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(query))
        results_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        if engine._is_future:
            conn.commit()
    return results_df


def execute_structural_command(query: str, engine: Engine) -> None:
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(text(query))


def get_data_schema_names(engine: Engine) -> List:
    insp = inspect(engine)
    return insp.get_schema_names()


def database_has_schema(engine: Engine, schema_name: str) -> bool:
    with engine.connect() as conn:
        return engine.dialect.has_schema(connection=conn, schema=schema_name)


def create_database_schema(engine: Engine, schema_name: str, verbose: bool = False) -> None:
    if not database_has_schema(engine=engine, schema_name=schema_name):
        with engine.connect() as conn:
            conn.execute(CreateSchema(name=schema_name))
            conn.commit()
            if verbose:
                print(f"Database schema '{schema_name}' successfully created.")
    else:
        if verbose:
            print(f"Database schema '{schema_name}' already exists.")


def get_data_table_names_in_schema(engine: Engine, schema_name: str) -> List:
    insp = inspect(engine)
    return insp.get_table_names(schema=schema_name)


def get_table_column_details(
    engine: Engine, schema_name: str, table_name: str, return_all_cols: bool = False
) -> pd.DataFrame:
    return_cols = [
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "column_default",
        "is_nullable",
        "data_type",
        "character_maximum_length",
        "character_octet_length",
        "numeric_precision",
        "numeric_precision_radix",
        "numeric_scale",
        "datetime_precision",
        "udt_catalog",
        "udt_schema",
        "udt_name",
        "dtd_identifier",
    ]
    if return_all_cols:
        return_cols_str = "*"
    else:
        return_cols_str = ", ".join(return_cols)

    query = f"""
        SELECT {return_cols_str}
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}'
        AND table_name = '{table_name}';
    """
    return execute_result_returning_query(query=query, engine=engine)


def get_geo_columns_in_table(engine: Engine, schema_name: str, table_name: str) -> List:
    table_col_details = get_table_column_details(
        engine=engine, schema_name=schema_name, table_name=table_name
    )
    geometry_column_names = [
        "geography",
        "geometry",
        "point",
        "lseg",
        "path",
        "box",
        "polygon",
        "line",
        "circle",
    ]
    col_is_geo_mask = table_col_details["udt_name"].str.lower().isin(geometry_column_names)
    if col_is_geo_mask.any():
        return table_col_details.loc[col_is_geo_mask, "column_name"].to_list()
    else:
        return list()


def is_geo_table(engine: Engine, schema_name: str, table_name: str) -> bool:
    geo_col_names = get_geo_columns_in_table(
        engine=engine, schema_name=schema_name, table_name=table_name
    )
    return len(geo_col_names) > 0


def get_srid_of_column(
    engine: Engine, schema_name: str, table_name: str, column_name: str = "geomout"
) -> int:
    """Returns the SRID (Spatial Reference ID) of a column, if it has a geo-type in this database.
    These values will typically be the EPSG (European Petroleum Survey Group) SRID number."""
    geo_col_names = get_geo_columns_in_table(
        engine=engine, schema_name=schema_name, table_name=table_name
    )
    if column_name in geo_col_names:
        column_srid = execute_result_returning_query(
            query=f"SELECT Find_SRID('{schema_name}', '{table_name}', '{column_name}');",
            engine=engine,
        )
        return column_srid["find_srid"].values[0]
    else:
        raise ValueError(
            f"Column {column_name} in {schema_name}.{table_name} is geometric or geographic "
            + f"(ie no SRID).\n Geo-columns in that table: {geo_col_names}"
        )
