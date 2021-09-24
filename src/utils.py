import os
import yaml

import psycopg2 as pg

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