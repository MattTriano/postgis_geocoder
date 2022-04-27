import os
from typing import Dict, List, Union
import yaml

import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine
from sqlalchemy import text


def get_project_root_dir() -> os.path:
    if "__file__" in globals().keys():
        root_dir = os.path.dirname(os.path.abspath("__file__"))
    else:
        root_dir = os.path.dirname(os.path.abspath("."))
    assert ".git" in os.listdir(root_dir)
    assert "postgis_geocoder" in root_dir
    return root_dir


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


def get_engine_from_secrets() -> Engine:
    connection_url = get_connection_url_from_secrets()
    engine = create_engine(connection_url, future=True)
    return engine


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
    connection_url = get_db_connection_url_from_credential_file(credential_path)
    return create_engine(connection_url)


def format_addresses_for_standardization(df: pd.DataFrame, addr_col: str) -> str:
    addrs_formatted_for_standardization = ", ".join([f"('{addr}')" for addr in df[addr_col].values])
    return addrs_formatted_for_standardization


def execute_result_returning_query(query: str, engine: Engine) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(query))
        results_df = pd.DataFrame(result.fetchall(), columns=result.keys())
        conn.commit()
    return results_df


def execute_structural_command(query: str, engine: Engine) -> pd.DataFrame:
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()


def write_addresses_to_temporary_address_table(addr_df: pd.DataFrame, engine: Engine) -> None:
    assert addr_df.shape[1] == 1, "Only one column permitted for addr_df."
    assert addr_df.columns[0] == "full_address", "Only 'full_address' permitted in addr_df."
    execute_structural_command(query="DROP TABLE IF EXISTS temporary_address_table;", engine=engine)
    addr_df.to_sql(name="temporary_address_table", con=engine, index=False)


def setup_temporary_address_table_schema_for_addr_normalization(engine: Engine) -> None:
    execute_structural_command(
        query="""
            ALTER TABLE temporary_address_table
                ADD COLUMN address integer DEFAULT NULL,
                ADD COLUMN predirabbrev varchar(5) DEFAULT NULL,
                ADD COLUMN streetname varchar(50) DEFAULT NULL,
                ADD COLUMN streettypeabbrev varchar(10) DEFAULT NULL,
                ADD COLUMN postdirabbrev varchar(5) DEFAULT NULL,
                ADD COLUMN internal varchar(20) DEFAULT NULL,
                ADD COLUMN location varchar(50) DEFAULT NULL,
                ADD COLUMN stateabbrev varchar(2) DEFAULT NULL,
                ADD COLUMN zip varchar(5) DEFAULT NULL,
                ADD COLUMN parsed boolean DEFAULT NULL,
                ADD COLUMN zip4 varchar(4) DEFAULT NULL,
                ADD COLUMN address_alphanumeric varchar(10) DEFAULT NULL;
        """,
        engine=engine,
    )


def normalize_temporary_address_table_addr_batch(engine: Engine, batch_size: int = 100) -> None:
    query = f"""
        UPDATE temporary_address_table
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
                FROM temporary_address_table
                WHERE streetname IS NULL LIMIT {batch_size}
            ) AS a
            LEFT JOIN LATERAL
            normalize_address(a.full_address) AS na
            ON true
        WHERE a.full_address = temporary_address_table.full_address;
    """
    execute_structural_command(query=query, engine=engine)


def normalize_temporary_address_table_addrs(engine: Engine, batch_size: int = 100) -> None:
    setup_temporary_address_table_schema_for_addr_normalization(engine=engine)
    normalize_temporary_address_table_addr_batch(engine=engine, batch_size=batch_size)


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
