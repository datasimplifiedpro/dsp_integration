# db_utils.py

import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine, text
from app_config import DB_CONFIG

def get_mysql_engine(host, port, database, user, password):
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
        print(f"Created Engine to {database} at {host}:{port} as {user}")
        return engine
    except Error as e:
        print("Error creating engine:", e)
        return None

def get_mysql_connection(host, port, database, user, password, allow_public_key=True, use_ssl=True):
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
#            allow_public_key_retrieval=allow_public_key,
            ssl_disabled=not use_ssl
        )
        if conn.is_connected():
            print(f"Connected to {database} at {host}:{port} as {user}")
            return conn
    except Error as e:
        print("Error connecting to MySQL:", e)
        return None

# Generic Utilities

# create engine
#   Use run config to pick the right configuration
engine = get_mysql_engine(**DB_CONFIG)

# clear staging table
def clear_staging_table(table_name: str):
    # Empty staging table
    truncate_sql = f"""truncate ul_staging.{table_name};"""

    with engine.begin() as conn:
        conn.execute(text(truncate_sql))

    return None
