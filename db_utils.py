# db_utils.py

# test Update to sqlalchemy
# this will get rid of UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested. Please consider using SQLAlchemy.
# https://mariadb.com/resources/blog/using-sqlalchemy-with-mariadb-connector-python-part-1/
# https://www.slingacademy.com/article/sqlalchemy-how-to-connect-to-mysql-database/
# To turn off warnings:
# import warnings
# warnings.simplefilter(action='ignore', category=UserWarning)

import warnings
import mysql.connector
from mysql.connector import Error
# import mariadb ## simply using mysql to connect to mariadb
from sqlalchemy import create_engine

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