# python libs
import logging
from datetime import datetime
import asyncio


# my libs
from etl_utils.decorator import log_etl_job
from etl_utils.logger import ETLLogger
from get_sample_utils import get_db_integration,get_api_sample,detect_column_types,create_exectute_table_sql
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


# list of Club IDs to substitute into the API URL
integration_df = get_db_integration()

url = integration_df['base_url'].iloc[0]

params = {
            "url": url
        }

@log_etl_job("get_api_sample")
def run_etl(parameters, run_id=None, start_time=None):
    df= get_api_sample(url)
    df["etlrunid"] = run_id

    print(len(df))
    if not df.empty:
        column_type= detect_column_types(df)
        table_name = "mb_clients"
        create_exectute_table_sql(table_name, column_type)

        # Convert fields
        df = field_converter(
            df,
            # cols_to_num=[],
            # cols_to_date=[],
            # cols_to_datetime=[],
            cols_to_bool=['isActive']
        )
        df = rename_campaigns_columns(df)
        clear_staging_table("abc_campaigns")
        df.to_sql("abc_campaigns", schema="staging", con=engine, index=False, if_exists="append")
        upsert_campaigns()
    return {"record_count": len(df)}


run_etl(parameters=params)
