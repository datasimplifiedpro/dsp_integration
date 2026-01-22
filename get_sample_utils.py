# abc_club_utils.py
from datetime import datetime

# python libs
import requests
import pandas as pd
import numpy as np
from pandas import DataFrame
from pymysql import Error
from six import integer_types
from sqlalchemy import text
from get_secret_utils import get_1p_secret
from token_generator import get_valid_token


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




def get_db_integration(id):
    try:
        df = pd.read_sql(f"select * from vw_integration where integration_id = {id}", con=engine)
    except Error as e:
        print("Error reading vw_integration:", e)
    return df

def create_api_header():
    # list of Club IDs to substitute into the API URL
    integration_df = get_db_integration()

    vaultid = integration_df['vault_id'].iloc[0]
    itemid = integration_df['item_id'].iloc[0]

    # This is what we will use for fetching fields from a specific vault and item
    creds = asyncio.run(get_1p_secret(vaultid, itemid))

    auth = get_valid_token()
    credential = creds.get('credential')
    site_id = creds.get('site_id')

    hdr = integration_df['header'].iloc[0]
    print(hdr)

    fhdr = hdr.format(credential=credential, site_id=site_id, auth=auth)
    print(fhdr)

    header = {item.strip() for item in fhdr.split(",")}
    print(header)

    headers = {
        "Accept": "application/json",
        "Api-Key": creds.get('credential'),
        "SiteId": creds.get('site_id'),
        "authorization": auth,
        }
    print(headers)

    return headers


# Function to create the campaigns data
def get_api_sample(url):

    headers = create_api_header()
    # Make the request
    response = requests.get(url, headers=headers)
    resp_data = response.content.replace(b'\xc3\xa2\xc2\x80\xc2\x99', b'''''')

    flattened_df: DataFrame
    next_page: str = '0'

    # if the request was successful, parse and save to df
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        flattened_df = pd.json_normalize(data['Clients'])
        # print(flattened_df)

        # Check list of dataframe columns
        column_list = flattened_df.columns.values.tolist()
        print(column_list)

        return flattened_df
    else:
        print(f"Failed to retrieve data for ID {url}, Status Code: {response.status_code}")
        return flattened_df


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

    df = df.replace({np.nan: None, pd.NaT: None})

    return df


# rename columns
def rename_campaigns_columns(df):
    # rename columns, remove personal and shorten to be valid db column name
    df.rename(columns={'isActive': 'active',
                       'id': 'campaignid',
                       'name': 'campaignname'}, inplace=True)
    return df


# # clear staging campaigns
# def clear_staging_campaigns():
#     # Empty staging table
#     truncate_sql = """truncate staging.abc_campaigns;"""
#     with engine.begin() as conn:
#         conn.execute(text(truncate_sql))
#
#     return

# Upsert campaigns
def upsert_campaigns():
    # update the items data from staging
    # Perform UPSERT into target table
    # updat the rows and columns
    upsert_sql = """
                 INSERT INTO abc_campaigns (etlrunid, clubid, active, campaignid, campaignname, type)
                 SELECT etlrunid, clubid, active, campaignid, campaignname, type \
                 FROM staging.abc_campaigns c ON DUPLICATE KEY \
                 UPDATE \
                     etlrunid = c.etlrunid, \
                     active = c.active, \
                     campaignname = c.campaignname, \
                     type = c.type; \

                 """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))

    return