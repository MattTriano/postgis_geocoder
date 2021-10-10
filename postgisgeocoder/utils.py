import os
from typing import Dict, List, Union
import yaml

import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine


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


def execute_result_returning_query(
    query: str, conn: pg.extensions.connection
) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    results_df = pd.DataFrame(
        results, columns=[el[0] for el in cur.description]
    )
    cur.close()
    return results_df


def get_standardized_address_df(
    conn: pg.extensions.connection, formatted_addrs: str
) -> pd.DataFrame:
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
    results_df = execute_result_returning_query(query, conn)
    return results_df


def func_timer(func):
    @functools.wraps(func)
    def wrapper_func_timer(*args, **kwargs):
        start_time = time.perf_counter()
        func_product = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Execution time: {run_time:0.4f} seconds")
        return func_product

    return wrapper_func_timer


def geocode_addr(
    conn: pg.extensions.connection,
    addr_to_geocode: str,
    top_n: Union[int, None] = None,
    restrict_geom_query: Union[str, None] = None,
) -> pd.DataFrame:
    if top_n is None:
        top_n = ""
    else:
        top_n = f", max_results := {top_n}"
    if restrict_geom_query is None:
        restrict_geom_query = ""
    else:
        restrict_geom_query = f", restrict_geom := ({restrict_geom_query})"

    query = f"""
    SELECT
        g.rating AS rating,
        ST_Y(g.geomout)::numeric(10,6) AS latitude,
        ST_X(g.geomout)::numeric(10,6) AS longitude,
        pprint_addy(addy) AS geocoded_address,
        (addy).address AS street_num,
        (addy).predirabbrev AS street_dir,
        (addy).streetname AS street_name,
        (addy).streettypeabbrev AS street_type,
        (addy).location AS city,
        (addy).stateabbrev AS st,
        (addy).zip AS zip
    FROM geocode(
        '{addr_to_geocode}'{top_n}{restrict_geom_query}
    ) As g;
    """
    geocode_results_df = execute_result_returning_query(query, conn)
    geocode_results_df["raw_address"] = addr_to_geocode
    return geocode_results_df


def geocode_list_of_addresses(
    conn: pg.extensions.connection,
    addrs_to_geocode: List[str],
    top_n: Union[int, None] = None,
    restrict_geom_query: Union[str, None] = None,
    print_every_n: int = 50,
) -> pd.DataFrame:
    num_addrs_left = len(addrs_to_geocode)
    print(f"Total number of addresses to geocode: {num_addrs_left}.")
    full_geocoded_results = []
    for addr_to_geocode in addrs_to_geocode:
        geocode_results_df = geocode_addr(
            conn=conn,
            addr_to_geocode=addr_to_geocode,
            top_n=top_n,
            restrict_geom_query=restrict_geom_query,
        )
        geocode_results_df["addr_similarity_ratio"] = geocode_results_df[
            "geocoded_address"
        ].apply(lambda x: similar(addr_to_geocode, x.upper()))
        num_addrs_left = num_addrs_left - 1
        full_geocoded_results.append(geocode_results_df)
        if num_addrs_left % print_every_n == 0:
            print(f"Number of addresses to geocode left: {num_addrs_left}.")
    full_geocoded_results_df = pd.concat(full_geocoded_results)
    full_geocoded_results_df = full_geocoded_results_df.reset_index(drop=True)
    return full_geocoded_results_df
