import os
from typing import Dict, List, Union
import yaml

import pandas as pd
import psycopg2 as pg
import shapely
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine


def get_project_root_dir() -> os.path:
    root_dir = os.path.dirname(os.path.dirname(__file__))
    assert os.path.basename(root_dir) == "postgis_geocoder"
    return root_dir


def format_addresses_for_standardization(df: pd.DataFrame, addr_col: str) -> str:
    addrs_formatted_for_standardization = ", ".join([f"('{addr}')" for addr in df[addr_col].values])
    return addrs_formatted_for_standardization


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


def coerce_postgis_geom_valued_string_to_gpd_geom(geom_str: str) -> shapely.geometry:
    if geom_str is not None:
        return shapely.wkb.loads(geom_str, hex=True)
    else:
        return None


def decode_geom_valued_column_to_geometry_type(series: pd.Series) -> pd.Series:
    return pd.Series(map(coerce_postgis_geom_valued_string_to_gpd_geom, series))


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
        geocode_results_df["addr_similarity_ratio"] = geocode_results_df["geocoded_address"].apply(
            lambda x: similar(addr_to_geocode, x.upper())
        )
        num_addrs_left = num_addrs_left - 1
        full_geocoded_results.append(geocode_results_df)
        if num_addrs_left % print_every_n == 0:
            print(f"Number of addresses to geocode left: {num_addrs_left}.")
    full_geocoded_results_df = pd.concat(full_geocoded_results)
    full_geocoded_results_df = full_geocoded_results_df.reset_index(drop=True)
    return full_geocoded_results_df
