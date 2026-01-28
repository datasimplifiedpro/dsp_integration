# python libs
import logging
from datetime import datetime

import json
import asyncio


# my libs
from etl_utils.decorator import log_etl_job
from etl_utils.logger import ETLLogger
from get_sample_utils import (get_db_integration,get_api_sample,detect_column_types,create_exectute_table_sql,get_db_tabel_metadata,
                              clear_staging_meta,upsert_metadata,field_converter, create_api_header, upsert_data_sample, convert_columns_sample, clear_staging_table)
from app_config import DB_CONFIG
from db_utils import get_mysql_engine

# create engine to import data
engine = get_mysql_engine(**DB_CONFIG)

# show start of program
rightnow = datetime.now()
print('Starting here!')
print(f'Start time: {rightnow}')

# instantiate logger class and retrieve last run parameters
#   this logic needs to be verified against the data in the table to ensure that the last run
#   was not a rerun of previous failed run
logger = ETLLogger("get_api_sample")
# last_run_params = logger.get_last_run(status='success')

# print("Last parameters:", last_params)
# print("Last parameters end date:", last_run_params['end_dt_str'])


# Set the log level here
logging.basicConfig(
    filename='log/app.log',
    level=logging.DEBUG,  # Log level set to DEBUG (log everything)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Log messages with different levels
# logging.debug('This is a debug message')     # Will be logged
# logging.info('This is an info message')      # Will be logged
# logging.warning('This is a warning message') # Will be logged
# logging.error('This is an error message')    # Will be logged
# logging.critical('This is a critical message')# Will be logged

logging.debug('Get API sample starting here!')


integration_df = get_db_integration(1)

application_name = integration_df['application_name'].iloc[0]
integration_id = integration_df['integration_id'].iloc[0]
integration_name = integration_df['integration_name'].iloc[0]
table_name = 'smpl_' + integration_df['table_name'].iloc[0]
app_header_template = integration_df['header'].iloc[0]
client_id = integration_df['client_id'].iloc[0]
client_name = integration_df['client_name'].iloc[0]
base_url_template = integration_df['base_url'].iloc[0]
data_node_name = integration_df['data_node_name'].iloc[0]
integration_pattern = integration_df['integration_pattern'].iloc[0]
pattern_table = integration_df['pattern_table'].iloc[0]
pattern_return_column = integration_df['pattern_return_column'].iloc[0]
pattern_where = integration_df['pattern_where'].iloc[0]
pattern_size = integration_df['pattern_size'].iloc[0]
vaultid = integration_df['vault_id'].iloc[0]
itemid = integration_df['item_id'].iloc[0]

url = base_url_template

headers = create_api_header(app_header_template, vaultid, itemid)

db_name = 'api_metadata'

params = {
            "url": url
        }

@log_etl_job(f"get_api_sample {application_name}-{integration_name}")
def run_etl(parameters, run_id=None, start_time=None):

    df= get_api_sample(url, headers, data_node_name)
    df["etlrunid"] = run_id

    print(len(df))
    if not df.empty:
        column_type= detect_column_types(df)
        table_name = integration_name
        create_exectute_table_sql(table_name, column_type)

        meta_df = get_db_tabel_metadata(table_name, db_name)
        meta_df["integration_id"] = integration_id

        # Convert fields
        meta_df = field_converter(
            meta_df,
            # cols_to_num=[],
            # cols_to_date=[],
            # cols_to_datetime=[],
            cols_to_bool=['IS_NULLABLE']
        )

        clear_staging_meta(db_name)

        meta_df.to_sql("staging_integration_columns", schema=db_name, con=engine, index=False, if_exists="append")
        # upsert_metadata()

        df = convert_columns_sample(df, client_id, integration_id)

        staging_table =f"staging_{integration_name}"

        clear_staging_table(db_name, integration_name)

        df.to_sql(staging_table, schema=db_name, con=engine, index=False, if_exists="append")
        upsert_data_sample(client_id, integration_id, integration_name, db_name)

    return {"record_count": len(df)}

run_etl(parameters=params)
