import os
import yaml

import psycopg2 as pg
from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.schema import Table

def get_db_connection_from_credential_file(credential_path: os.path) -> pg.extensions.connection:
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


def get_db_connection_url_from_credential_file(credential_path: os.path) -> URL:
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
        port=credentials["port"]
    )


def get_engine_from_credential_file(credential_path: str) -> Engine:
    """Returns an sqlalchemy engine with the permissions corresponding to 
    the credentials in the file at credential_path."""
    connection_url = get_db_connection_url_from_credential_file(credential_path)
    return create_engine(connection_url)