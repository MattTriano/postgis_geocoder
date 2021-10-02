import os
from typing import Dict, List, Union
import yaml

import psycopg2 as pg
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.schema import Table


def get_db_connection_from_credential_file(
    credential_path: os.path,
) -> pg.extensions.connection:
    with open(credential_path) as cred_file:
        credentials = yaml.load(cred_file, Loader=yaml.FullLoader)

    conn = pg.connect(
        dbname=credentials["database"],
        user=credentials["username"],
        password=credentials["password"],
        port=credentials["port"],
        host=credentials["host"],
    )
    conn.set_session(autocommit=True)
    return conn


def get_db_connection_url_from_credential_file(
    credential_path: os.path,
) -> URL:
    """Returns a connection string with the permissions corresponding to
    the credentials in the file at credential_path."""
    with open(credential_path) as cred_file:
        credentials = yaml.load(cred_file, Loader=yaml.FullLoader)

    return URL.create(
        drivername=credentials["driver"],
        host=credentials["host"],
        username=credentials["username"],
        database=credentials["database"],
        password=credentials["password"],
        port=credentials["port"],
    )


def get_engine_from_credential_file(credential_path: str) -> Engine:
    """Returns an sqlalchemy engine with the permissions corresponding to
    the credentials in the file at credential_path."""
    connection_url = get_db_connection_url_from_credential_file(
        credential_path
    )
    return create_engine(connection_url)


def format_addresses_for_standardization(
    df: pd.DataFrame, addr_col: str
) -> str:
    addrs_formatted_for_standardization = ", ".join(
        [f"('{addr}')" for addr in df[addr_col].values]
    )
    return addrs_formatted_for_standardization


def get_standardized_address_df(
    conn: pg.extensions.connection, formatted_addrs: str
) -> pd.DataFrame:
    cur = conn.cursor()
    query = f"""
    WITH A(a) AS (
        VALUES 
            {formatted_addrs}
    )
    SELECT (s).house_num, (s).predir, (s).name, (s).suftype, 
           (s).sufdir, (s).city, (s).state
    FROM (
        SELECT standardize_address(
            'pagc_lex','pagc_gaz','pagc_rules', a
        ) As s FROM A
    ) AS X;
    """

    cur.execute(query)
    results = cur.fetchall()
    result_df = pd.DataFrame(
        results, columns=[el[0] for el in cur.description]
    )
    cur.close()
    return result_df
