# abc_club_utils.py
from datetime import datetime

# python libs
import requests
import pandas as pd
import numpy as np
import re
from pandas import DataFrame
from pymysql import Error
from six import integer_types
from sqlalchemy import text
from get_secret_utils import get_1p_secret
from token_generator import get_valid_token
import json
from fnmatch import fnmatch

# my libs
from app_config import DB_CONFIG, ONEP_HEADER
from db_utils import get_mysql_engine
import asyncio

# create engine
#   Use run config to pick the right configuration
engine = get_mysql_engine(**DB_CONFIG)

"""
name:  abc_club_utils.py
purpose:
a place to have reusable abc API calls for clubs

Note:
Author: Marty Afkhami
Created: 2025-04-10

Updates:

20250410 - MMA - Created Original VERSION

"""
# ------------------------------------------------------------------------


sql = """call AnalyzeTableColumns('staging','smpl_mb_sales')"""

def detect_mysql_type(series):
    if series.dropna().empty:
        return 'VARCHAR(255)'

    # Get first non-null value
    val = str(series.dropna().iloc[0]).strip()

    # Regex Patterns
    patterns = {
        # Matches: 2026-01-28 15:54:06, 2026-01-28T15:54Z, 2026-01-28 15:54
        'DATETIME': r'^\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}(?::\d{2})?Z?$',

        # Matches: 2026-01-28, 01/28/2026, Jan 28, 2026
        'DATE': [
            r'^\d{4}-\d{2}-\d{2}$',
            r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$',
            r'^[a-zA-Z]{3,9}[\s-]\d{1,2}[,\s-]+\d{4}$'
        ],

        # Matches: 15:54:06, 11:34, 11:34 AM, 11:34:56PM
        'TIME': r'^\d{1,2}:\d{2}(?::\d{2})?(\s?[AP]M)?$'
    }

    for db_type, regex in patterns.items():
        if isinstance(regex, list):
            for r in regex:
                if re.match(r, val, re.IGNORECASE):
                    return db_type
        else:
            if re.match(regex, val, re.IGNORECASE):
                return db_type

    return 'VARCHAR(255)'


# --- Test Scenarios ---
test_cases = {
    'standard_sql': ['2026-01-09 00:17:58'],
    'short_time': ['11:34'],
    'ampm_time': ['11:34 PM'],
    'iso_no_seconds': ['2026-01-28T15:54Z']
}

for name, val in test_cases.items():
    print(f"{name}: {detect_mysql_type(pd.Series(val))}")


data = {
    'col_a': ['2026-01-28T15:00:00Z'], # DATETIME
    'col_b': ['01/28/2026'],            # DATE
    'col_c': ['January 28, 2026'],      # DATE
    'col_d': ['12:30:45']               # TIME
}

df = pd.DataFrame(data)
for col in df.columns:
    print(f"Column '{col}' should be MySQL type: {detect_mysql_type(df[col])}")
# ------------------------------------------------------------------------


def get_db_integration_sample(id):
    try:
        df = pd.read_sql(f"select * from vw_integration_sample where integration_id = {id}", con=engine)
    except Error as e:
        print("Error reading vw_integration:", e)
    return df

def create_api_header(header_template, vaultid, itemid):

    # This is what we will use for fetching fields from a specific vault and item
    creds = asyncio.run(get_1p_secret(vaultid, itemid))

    header = header_template.format(**creds)

    headers = json.loads(header)

    # # auth = get_valid_token()
    # credential = creds.get('credential')
    # site_id = creds.get('site_id')
    # # {'Accept': 'application/json', 'Api-Key': '{credential}', 'Authorization': '{auth}', 'SiteId': '{site_id}'}
    #
    #
    # # "Accept": "application/json",
    # # "Api-Key": {credential},
    # # "SiteId": {site_id}
    #
    # headers = {
    #     k: v.format(credential=credential, site_id=site_id, auth=auth)
    #     for k, v in header_json.items()
    # }

    print(headers)
    return headers



def get_api_sample(url, header, node_name):

    response = requests.get(url, headers=header)
    resp_data = response.content.replace(b'\xc3\xa2\xc2\x80\xc2\x99', b'''''')

    flattened_df= pd.DataFrame()
    next_page: str = '0'

    # if the request was successful, parse and save to df
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # node_name = str(node_name).strip()

        flattened_df = pd.json_normalize(data[node_name], sep='_')
        # print(flattened_df)

        # Check list of dataframe columns
        column_list = flattened_df.columns.values.tolist()
        print(column_list)

        return flattened_df
    else:
        print(f"Failed to retrieve data for ID {url}, Status Code: {response.status_code}")
        return flattened_df


def detect_column_types(df):

    column_types = {}

    for col in df.columns:
        col_data = df[col].dropna()
        name = str(col).lower()

        # Empty column
        if len(col_data) == 0:
            column_types[col] = "Varchar(100)"
            continue

        # Keyword overrides (SQL LIKE '%keyword%')
        if fnmatch(name, "*id*"):
            column_types[col] = "VARCHAR(100)"
            continue

        if fnmatch(name, "*phone*"):
            column_types[col] = "VARCHAR(50)"
            continue

        # what about 'email_optin' column name?
        if fnmatch(name, "*email*"):
            column_types[col] = "VARCHAR(150)"
            continue

        if fnmatch(name, "*url*"):
            column_types[col] = "VARCHAR(250)"
            continue

        if fnmatch(name, "*zip*") or fnmatch(name, "*postal*"):
            column_types[col] = "VARCHAR(20)"
            continue

        # let's add address*, city

        dtype_str = str(df[col].dtype)

        # Sample data for checks
        sample_size = min(1000, len(col_data))
        sample = col_data.sample(n=sample_size, random_state=42) if len(col_data) > sample_size else col_data

        if 'int' in dtype_str:
            max_val = sample.abs().max()
            column_types[col] = "VARCHAR(100)" if max_val > 2147483647 else "INT"

        elif 'float' in dtype_str:
            nums = pd.to_numeric(sample, errors="coerce").dropna()
            if not nums.empty:
                max_abs = float(nums.abs().max())
                column_types[col] = "VARCHAR(100)" if max_abs > 99999999.99 else "DECIMAL(10,2)"
            else:
                column_types[col] = "DECIMAL(10,2)"

        elif 'bool' in dtype_str:
            column_types[col] = "BOOLEAN"

        elif 'datetime' in dtype_str:
            column_types[col] = "DATE" if (sample == sample.dt.normalize()).all() else "DATETIME"

        elif dtype_str == 'object':
            sample_str = sample.astype(str)

            # Try datetime
            try:
                parsed = pd.to_datetime(sample_str.head(100), errors='raise', utc=True)
                column_types[col] = "DATE" if (parsed == parsed.dt.normalize()).all() else "DATETIME"
                continue
            except:
                pass

            # Try numeric with special chars
            if sample_str.str.match(r'^[\$,\d\.\-\+\s]+$', na=False).mean() > 0.8:
                cleaned = sample_str.str.replace(r'[\$,\s]', '', regex=True)
                nums = pd.to_numeric(cleaned, errors='coerce').dropna()
                column_types[col] = "VARCHAR(100)" if (
                            not nums.empty and nums.abs().max() > 99999999.99) else "DECIMAL(10,2)"
                continue

            # Try boolean
            if sample_str.str.lower().isin(['true', 'false', 't', 'f', 'yes', 'no', 'y', 'n', '0', '1']).mean() > 0.9:
                column_types[col] = "BOOLEAN"
                continue

            # String - VARCHAR or TEXT
            max_len = sample_str.str.len().max()
            varchar_size = int(max_len + 50)
            column_types[col] = "TEXT" if varchar_size > 255 else f"VARCHAR({min(varchar_size, 255)})"

        else:
            column_types[col] = "TEXT"

    return column_types

def create_execute_table_sql(db_name, table_name, column_types):

    column_definitions = []

    # Add auto-increment primary key first
    column_definitions.append("idnum INT AUTO_INCREMENT PRIMARY KEY")

    # Add all DataFrame columns and pass primary key with its data type if we want to
    for col_name, mysql_type in column_types.items():
        # if col_name == primary_key:
        #     column_definitions.append(f"`{col_name}` {mysql_type} PRIMARY KEY")
        # else:
        column_definitions.append(f"`{col_name}` {mysql_type}")

    # create_sql_main = f"""
    #     CREATE TABLE IF NOT EXISTS `{db_name}.{table_name}` (
    #         {', '.join(column_definitions)}
    #     )
    #     """
    create_sql_staging = f"""
             CREATE TABLE IF NOT EXISTS {db_name}.`{table_name}` (
                {', '.join(column_definitions)}
            )
            """

    with engine.begin() as conn:
        # conn.execute(text(create_sql_main))
        conn.execute(text(create_sql_staging))
    print(f"✓ Table '{table_name}' created successfully")


def get_db_table_metadata(table_name, db_name, integration_id, client_id=0):

    try:
        df = pd.read_sql(f"""
            insert
                into
                api_metadata.integration_columns(client_id,
                integration_id,
                api_column_name,
                column_name,
                ORDINAL_POSITION,
                is_nullable,
                COLUMN_DEFAULT,
                COLUMN_TYPE,
                df_column_type,
                COLUMN_KEY,
                EXTRA,
                load_at_runtime,
                active)
            select
                {client_id} as client_id,
                {integration_id} as integration_id,
                column_name as api_column_name,
                replace(replace(lower(column_name), ' ', '_'), '.', '_') as column_name,
                ordinal_position,
                case
                    is_nullable when 'YES' then 1
                    else 0
                end as is_nullable,
                column_default,
                column_type,
                case column_type 
                    when 'tinyint' then 'bool'
                    when 'int' then 'numeric'
                    when 'int unsigned' then 'numeric'
                    when 'double' then 'numeric'
                    when 'bigint' then 'numeric'
                    when 'date' then 'date'
                    when 'datetime' then 'datetime'
                end as df_column_type,
                column_key,
                extra,
                0 as load_at_runtime,
                1 as active
            from
                information_schema.columns
            where
                table_name = '{table_name}'
                and table_schema = '{db_name}'
            order by
                ordinal_position
            """, con=engine)
    except Error as e:
        print(f"Error extracting the metadata for table {table_name}", e)
    return df

# You can pass only the parameters you need
def field_converter(df, cols_to_num=None, cols_to_date=None,
                    cols_to_datetime=None, cols_to_bool=None):
    #    N U M E R I C    C O N V E R S I O N   #
    # This checks if cols_to_num is NOT None and NOT empty
    if cols_to_num:
        # Do the numeric conversion
        # Remove $ and , characters ex: 2,300 or $15,230.45 then convert to numeric
        df[cols_to_num] = df[cols_to_num].replace({r'[\$,]': ''}, regex=True)
        # Convert to numeric (float), handle errors as NaN
        df[cols_to_num] = df[cols_to_num].apply(pd.to_numeric, errors='coerce')

    #    D A T E   C O N V E R S I O N    #
    if cols_to_date:
        df[cols_to_date] = df[cols_to_date].apply(lambda col: pd.to_datetime(col, errors='coerce').dt.date)

    #    D A T E T I M E   C O N V E R S I O N    #
    if cols_to_datetime:
        df[cols_to_datetime] = df[cols_to_datetime].apply(lambda col: pd.to_datetime(col, errors='coerce'))

    #    B O O L E A N   C O N V E R S I O N    #
    if cols_to_bool:
        # single bool column having values of true and false
        # df['bool_col'] = (df['bool_col'].astype(str).str.strip().str.lower().map({'true': True, 'false': False}))
        # more generic boolean conversion
        bool_map = {
            'true': True, 't': True, 'yes': True, 'y': True, '1': True,
            'false': False, 'f': False, 'no': False, 'n': False, '0': False,
            np.True_: True, np.False_: False
        }

        # Normalize text: lowercase, stripped strings
        # df_norm = df[cols_to_bool].apply(lambda col: col.astype(str).str.strip().str.lower())
        df[cols_to_bool] = df[cols_to_bool].apply(lambda col: col.astype(str).str.strip().str.lower().map(bool_map))

        # Set of known valid strings
        valid_values = set(bool_map.keys())
        # Find unexpected boolean values in each column
        unexpected_values = {
            col: set(df[col].dropna().unique()) - valid_values
            for col in cols_to_bool
            if len(set(df[col].dropna().unique()) - valid_values) > 0
        }
        print('unexpected_values')
        print(unexpected_values)

        #    N E S T E D   J S O N   F I X   (dict/list -> JSON string)   #
    bad_cols = [
        c for c in df.columns
        if df[c].apply(lambda v: isinstance(v, (dict, list))).any()
    ]

    if bad_cols:
        for c in bad_cols:
            df[c] = df[c].apply(
                lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
            )

    df = df.replace({np.nan: None, pd.NaT: None})

    return df

# clear staging
def clear_staging_meta(db_name):
    # Empty staging table
    truncate_sql = f"""truncate {db_name}.staging_integration_columns;"""
    with engine.begin() as conn:
        conn.execute(text(truncate_sql))

    return


# Upsert
def upsert_metadata():
    # update the items data from staging
    # Perform UPSERT into target table
    # update the rows and columns
    upsert_sql = """
                 INSERT INTO integration_columns (integration_id, api_column_name, ordinal_position, column_default, is_nullable, column_type, column_key, extra )
                 SELECT integration_id, column_name as api_column_name, ordinal_position, column_default, is_nullable, column_type, column_key, extra 
                 FROM api_metadata.staging_integration_columns i ON DUPLICATE KEY 
                 UPDATE 
                     integration_id = i.integration_id,
                     api_column_name = i.api_column_name,
                     ordinal_position = i.ordinal_position,
                     column_default = i.column_default,
                     is_nullable = i.is_nullable,
                     column_type = i.column_type,
                     column_key = i.column_key,
                     extra = i.extra; 
                 """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))

    return

def get_df_column_types_sample(clientid, integrationid):
    try:
        df = pd.read_sql(f"select api_column_name, column_type from integration_columns where client_id = (select max(ifnull(client_id = {clientid},0))*{clientid} from integration_columns) and integration_id = {integrationid} and column_type not like 'varchar%%' and column_type <> 'text' order by column_type ", con=engine)
    except Error as e:
        print("Error reading abc club data:", e)
    return df

def get_db_insert_columns_sample(clientid, integrationid):
    try:
        df = pd.read_sql(f"select api_column_name from integration_columns where active and integration_id = {integrationid} and client_id = (select max(ifnull(client_id = {clientid},0))*{clientid} from integration_columns) order by ordinal_position", con=engine)
        result = ",".join(df['api_column_name'].astype(str))
    except Error as e:
        print("Error reading vw_integration:", e)
    return result

def get_db_update_columns_sample(clientid, integrationid):
    try:
        df = pd.read_sql(f"select api_column_name || ' = v.' || api_column_name as column_name from integration_columns where active and column_key <> 'PRI' and integration_id = {integrationid} and client_id = (select max(ifnull(client_id = {clientid},0))*{clientid} from integration_columns) order by ordinal_position", con=engine)
        result = ",".join(df['api_column_name'].astype(str))
    except Error as e:
        print("Error reading vw_integration:", e)
    return result

def get_upsert_sql_sample(clientid, integrationid, integrationname, db_name):
    insert_list = get_db_insert_columns_sample(clientid, integrationid)
    update_list = get_db_update_columns_sample(clientid, integrationid)
    sql_str = f'INSERT INTO {integrationname} (' + insert_list + ') select ' + insert_list + f' FROM {db_name}.{integrationname} v ON DUPLICATE KEY UPDATE ' + update_list + ';'
    return sql_str

# rename columns
def convert_columns_sample(df, clnid, intgid):


    convert_columns = get_df_column_types_sample(clnid, intgid)
    df = df.drop(columns=['idnum'], errors='ignore')

    filtered = convert_columns[(convert_columns['column_type'].isin(['int', 'decimal(10,2)', 'mediumint unsigned', 'int unsigned'])) & (convert_columns['api_column_name'] != 'idnum')]
    num_cols = filtered['api_column_name'].tolist()

    filtered = convert_columns[convert_columns['column_type'] == 'date']
    date_cols = filtered['api_column_name'].tolist()

    filtered = convert_columns[convert_columns['column_type'] == 'datetime']
    datetime_cols = filtered['api_column_name'].tolist()

    filtered = convert_columns[convert_columns['column_type']  == 'tinyint(1)']
    bool_cols = filtered['api_column_name'].tolist()

    df = field_converter(df, num_cols, date_cols, datetime_cols, bool_cols)

    return df


# clear staging items
def clear_staging_table(db_name, integration_name):
    # call before loading new rows, keep for troubleshooting
    truncate_sql = f"""truncate {db_name}.staging_{integration_name};"""

    with engine.begin() as conn:
        conn.execute(text(truncate_sql))

    return


# Upsert item categories
def upsert_data_sample(clientid, integrationid, integrationname, db_name):
    upsert_sql = get_upsert_sql_sample(clientid, integrationid, integrationname, db_name)
    with engine.begin() as conn:
        conn.execute( (text(upsert_sql)))

    return
