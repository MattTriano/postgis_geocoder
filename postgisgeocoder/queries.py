import pandas as pd
import psycopg2 as pg

from postgisgeocoder.utils import execute_result_returning_query


def get_user_geocode_settings(conn: pg.extensions.connection) -> pd.DataFrame:
    """Returns geocode_settings that were set by the user. If the user hasn't
    set or changed any settings, it will return an empty DataFrame.
    """
    query = f"""
        SELECT * FROM tiger.geocode_settings;
    """
    return execute_result_returning_query(query=query, conn=conn)

