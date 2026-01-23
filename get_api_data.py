# python libs
import logging
from datetime import datetime

# my libs
from etl_utils.decorator import log_etl_job
# from etl_utils.logger import ETLLogger
from get_sample_utils import get_db_integration,get_api_sample
from get_api_data_utils import get_db_data, get_api, convert_columns
from get_api_data_utils import upsert_data, rename_columns, get_db_expected_columns, audit_df_columns, normalize_df_columns
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
base_url_template = integration_df['base_url'].iloc[0]
data_node_name = integration_df['data_node_name'].iloc[0]
integration_pattern = integration_df['integration_pattern'].iloc[0]
pattern_table = integration_df['pattern_table'].iloc[0]
pattern_return_column = integration_df['pattern_return_column'].iloc[0]
pattern_where = integration_df['pattern_where'].iloc[0]
pattern_size = integration_df['pattern_size'].iloc[0]

# Set the log level here
logging.basicConfig(
    filename='./log/app.log',
    level=logging.DEBUG,  # Log level set to DEBUG (log everything)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logging.debug(f'{integration_name} for {client_name} starting here!')


# list of Club IDs to substitute into the API URL
loop_df = get_db_data(pattern_return_column, pattern_table, pattern_where)

# Step through each club's active member
for row in loop_df.itertuples(index=False):
    
    
    row_dict = row._asdict()
    loop_column = row_dict['loop_column']
    base_url = base_url_template.format(**row_dict)

    # Initialize the first page
    page = 1
    size = pattern_size  # Number of items per page
    total_items = 0

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
            # df["etl_start_time"] = start_time
            print(len(df))
            if not df.empty:
                # Add necessary columns
                df[pattern_return_column] = loop_column
                df["etlrunid"] = run_id

                df = rename_columns(df, client_id, integration_id)
                expected_columns = get_db_expected_columns(client_id, integration_id)
                missing, extra = audit_df_columns(df, expected_columns)
                if missing:
                    logging.debug(f'missing: {missing}')
                    print(f'missing: {missing}')
                if extra:
                    logging.debug(f'extra: {extra}')
                    print(f'extra: {extra}')
                df = normalize_df_columns(df, expected_columns, missing)
                df = convert_columns(df, client_id, integration_id)

                clear_staging_table(integration_name)
                df.to_sql(integration_name, schema="ul_staging", con=engine, index=False, if_exists="append")
                upsert_data(client_id, integration_id, integration_name)
            return {"record_count": len(df)}


        run_etl(parameters=params)

        # If no nextPage is provided, stop the loop
        if (not next_page) or (next_page == '0'):
            print(f"No more pages available for ID {loop_column}, stopping.")
            break

        # Move to the next page
        page = int(next_page)

