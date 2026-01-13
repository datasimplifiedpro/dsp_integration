# python libs
import logging
from datetime import datetime

# my libs
from etl_utils.decorator import log_etl_job
# from etl_utils.logger import ETLLogger
from get_sample_utils import get_db_integration,get_api_sample
from get_api_data_utils import get_db_abc_clubs, get_api, convert_columns
from get_api_data_utils import upsert_data, rename_columns
from app_config import DB_CONFIG
from db_utils import get_mysql_engine, clear_staging_table

# create engine to import data
engine = get_mysql_engine(**DB_CONFIG)

# show start of program
rightnow = datetime.now()
print('Starting here!')
print(f'Start time: {rightnow}')


# make a call to get the integration info
integration_df = get_db_integration(2)
application_name = integration_df['application_name'].iloc[0]
integration_id = integration_df['integration_id'].iloc[0]
integration_name = integration_df['integration_name'].iloc[0]
client_id = integration_df['client_id'].iloc[0]
client_name = integration_df['client_name'].iloc[0]
base_url = integration_df['base_url'].iloc[0]
data_node_name = integration_df['data_node_name'].iloc[0]


# Set the log level here
logging.basicConfig(
    filename='./log/app.log',
    level=logging.DEBUG,  # Log level set to DEBUG (log everything)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.debug(f'{integration_name} for {client_name} starting here!')


# list of Club IDs to substitute into the API URL
club_df = get_db_abc_clubs()

# Step through each club's active member
for row in club_df.itertuples():
    club = row.clubid    # Initialize
    # base_url = f"https://api.abcfinancial.com/rest/{club}/clubs/items/categories"

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

        @log_etl_job(f"{application_name}-{integration_name}")
        def run_etl(parameters, run_id=None, start_time=None):
            global next_page
            df, next_page = get_api(url, data_node_name)
            df["club"] = club
            df["etlrunid"] = run_id
            # df["etl_start_time"] = start_time
            print(len(df))
            if not df.empty:
                df = rename_columns(df, client_id, integration_id)
                df = convert_columns(df, client_id, integration_id)
                clear_staging_table(integration_name)
                df.to_sql(integration_name, schema="ul_staging", con=engine, index=False, if_exists="append")
                upsert_data(client_id, integration_id, integration_name)
            return {"record_count": len(df)}


        run_etl(parameters=params)

        # If no nextPage is provided, stop the loop
        if (not next_page) or (next_page == '0'):
            print(f"No more pages available for ID {club}, stopping.")
            break

        # Move to the next page
        page = int(next_page)

