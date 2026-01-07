# python libs
import logging
from datetime import datetime
import asyncio
from onepassword.client import Client


# my libs
from etl_utils.decorator import log_etl_job
from etl_utils.logger import ETLLogger
from get_sample_utils import get_db_integration, get_1p_secret
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
logger = ETLLogger("get_api_abc_campaigns")
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

logging.debug('Vida campaigns starting here!')


# list of Club IDs to substitute into the API URL
integration_df = get_db_integration()
# creds_df = await get_1p_secret("Tonehouse", "MINDBODY PROD API Credentials")
creds_df = asyncio.run(get_1p_secret("Tonehouse", "MINDBODY PROD API Credentials "))

credential = creds.get('credential')
site_id = creds.get('site_id')
version = creds.get('version')

# Step through each club's active member
for row in club_df.itertuples():
    club = row.clubid    # Initialize
    base_url = f"https://api.abcfinancial.com/rest/{club}/clubs/campaigns"

    # Initialize the first page
    page = 1
    size = 1000  # Number of items per page
    total_items = 0
    print(f"club: {club}")

    while True:

        # Construct the URL for the current page
        url = f"{base_url}?page={page}&size={size}"

        params = {
            "page": page,
            "size": size,
            "url": url
        }

        global next_page

        @log_etl_job("get_api_abc_club_campaigns")
        def run_etl(parameters, run_id=None, start_time=None):
            global next_page
            df, next_page = get_api_abc_club_campaigns(url)
            df["clubid"] = club
            df["etlrunid"] = run_id
            # df["etl_start_time"] = start_time
            print(len(df))
            if not df.empty:
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

        # If no nextPage is provided, stop the loop
        if (not next_page) or (next_page == '0'):
            print(f"No more pages available for ID {club}, stopping.")
            break

        # Move to the next page
        page = int(next_page)

