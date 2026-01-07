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

# my libs
from app_config import DB_CONFIG, ONEP_HEADER
from db_utils import get_mysql_engine
import asyncio
from onepassword.client import Client
# create engine
#   Use run config to pick the right configuration
engine = get_mysql_engine(**DB_CONFIG)

# create abc api headers
# headers = {**ABC_API_HEADER}

"""

name:  abc_club_utils.py
purpose:
a place to have reusable abc API calls for clubs


Note:

Author: Marty Afkhami
Created: 2025-04-10

Updates:

20250410 - MMA - Created Original VERSION
20250410 - MMA - added get club list from db
20250411 - MMA - Read secret db connection and api headers from app_config
20250412 - MMA - Updated to use sqlalchemy engine
20250413 - MMA - added get pos transactions for club
20250414 - MMA - added get club items
20250414 - MMA - added get plans
20250516 - MMA - added get plan details
20250605 - MMA - continue pos transactions for club routines
20250702 - MMA - updated get clubs function to pull club as clubid



"""


def get_db_integration():
    try:
        df = pd.read_sql("select * from vw_integration", con=engine)
    except Error as e:
        print("Error reading vw_integration:", e)
    return df


def get_api_abc_club_pos_transacts(url):
    # str_sql = f"select ifnull(memberid,'{membID}') as memberid, ifnull(DATE_ADD(date(max(ph.purchaseDate)), INTERVAL 1 DAY), '1990-01-01') as max_date from purchase_history_club ph where memberid = '{membID}'"
    # df_max_pur_dt = pd.read_sql(str_sql, read_conn)

    # Make the request
    response = requests.get(url, headers=headers)
    resp_data = response.content.replace(b'\xc3\xa2\xc2\x80\xc2\x99', b'''''')

    flattened_df: DataFrame
    next_page: str = '0'

    # if the request was successful, parse and save to df
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        # Get the count and nextPage from the response
        status = data.get("status", {})
        count = int(status.get("count", 0))  # Convert count to integer
        next_page = status.get("nextPage")  # Get nextPage if available

        if count > 0:
            purchase_hist = data.get("clubs", [])[0].get("transactions", [])
        else:
            purchase_hist = []
        # print(purchase_hist)

        # Normalize and flatten the structure: explode items and payments
        records = []
        for txn in purchase_hist:
            txn_base = {k: v for k, v in txn.items() if k != "items"}
            items = txn.get("items", {}).get("item", [])
            for item in items:
                item_base = {k: v for k, v in item.items() if k != "payments"}
                payments = item.get("payments", [])
                for payment in payments:
                    # Combine transaction, item, and payment data
                    record = {
                        **txn_base,
                        **item_base,
                        **payment
                    }
                    records.append(record)

        # Create DataFrame
        flattened_df = pd.DataFrame(records)

        return flattened_df, next_page

        # num_purchases, num_columns = flattened_df.shape
        # total_sales += num_purchases
        #
        # # either Log or write to file (see below)
        # if num_purchases > 0:
        #     print("\n", f"total: {total_sales}: sales {num_purchases}")
        #     flattened_df = convert_types(flattened_df)
        #     flattened_df.to_sql('transactions', con=engine, index=False, if_exists='replace')
        # # print("\r", f"{row_num}: Processing {num_purchases} From {start_dt} to {end_dt} for {membID}", end="")

    #         # Check list of dataframe columns
    #         column_list = flattened_df.columns.values.tolist()
    #         # print(column_list)
    #
    #         # Check if the DataFrame has rows
    #         df_has_rows = not flattened_df.empty  # can use shape below
    #         # print(f"DataFrame has rows: {df_has_rows}")
    #
    #         # If count is 0, stop the loop
    #         if count == 0:
    #             print(f"No more members to process for ID {club}, stopping.")
    #             break
    #
    #         # If no nextPage is provided, stop the loop
    #         if not next_page:
    #             print(f"No more pages available for ID {club}, stopping.")
    #             break
    #
    #         page = int(next_page)
    #
    else:
        print(f"Failed to retrieve data for ID {url}, Status Code: {response.status_code}")


# convert dataframe column types prior to writing to db
#   should all column type conversions be in one routine?
def convert_transaction_types(df):
    # Converting specific fields to the correct type

    df = df.replace({'Unlimited': '-1', r'[\$,]': ''}, regex=True)

    # Convert to correct types
    df['transactionTimestamp'] = pd.to_datetime(df['transactionTimestamp'], errors='coerce')
    df['homeClub'] = pd.to_numeric(df['homeClub'], errors='coerce')
    # df["homeClub"] = df["homeClub"].astype("Int32")
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unitPrice'] = pd.to_numeric(df['unitPrice'], errors='coerce')
    df['subtotal'] = pd.to_numeric(df['subtotal'], errors='coerce')
    df['tax'] = pd.to_numeric(df['tax'], errors='coerce')
    df['paymentAmount'] = pd.to_numeric(df['paymentAmount'], errors='coerce')
    df['paymentTax'] = pd.to_numeric(df['paymentTax'], errors='coerce')

    # Boolean conversion
    df['return'] = df['return'].astype(bool)
    df['sale'] = df['sale'].astype(bool)

    # Convert dates and timestamps
    # personal['birthDate'] = pd.to_datetime(personal.get('birthDate', None), errors='coerce')
    # personal['firstCheckInTimestamp'] = pd.to_datetime(personal.get('firstCheckInTimestamp', None), errors='coerce')

    # Convert to numeric with coercion
    # df['A'] = pd.to_numeric(df['A'], errors='coerce')

    # Convert to Decimal, handling NaN values
    # df['purchaseDate'] = pd.to_datetime(df['purchaseDate'], errors='coerce')
    # df['expirationDate'] = pd.to_datetime(df['expirationDate'], errors='coerce')
    # df["expirationDate"] = df["expirationDate"].dt.date
    # df["expirationDate"] = df["expirationDate"].replace({pd.NaT: None})

    # df["expirationDate"] = df["expirationDate"].where(df["expirationDate"].notna(), None)
    # df["expirationDate"] = df["expirationDate"].astype(object).fillna(None)

    # Convert to numeric, handling NaN values
    # df['totalPrice'] = pd.to_numeric(df['totalPrice'], errors='coerce')
    # df['purchased'] = pd.to_numeric(df['purchased'], errors='coerce')
    # df['unavailable'] = pd.to_numeric(df['unavailable'], errors='coerce')
    # df['scheduled'] = pd.to_numeric(df['scheduled'], errors='coerce')
    # df['available'] = pd.to_numeric(df['available'], errors='coerce')
    # df['unscheduled'] = pd.to_numeric(df['unscheduled'], errors='coerce')

    # Convert boolean-like strings to actual booleans
    # df['deductible'] = df['deductible'].map({'True': True, 'False': False})
    # df['availableForBooking'] = df['availableForBooking'].map({'True': True, 'False': False})
    # df['mobile'] = df['mobile'].map({'True': True, 'False': False})

    # print(df.dtypes)

    # Convert NaN to None
    df = df.replace({np.nan: None, pd.NaT: None, 'Unlimited': '-1'})

    return df


# rename dataframe columns to match db prior to writing to db
#   should all column renames be in one routine?
def rename_transaction_columns(df):
    # rename columns, remove personal and shorten to be valid db column name
    df.rename(columns={'etlRunId': 'etlrunid',
                       'transactionId': 'transactionid',
                       'transactionTimestamp': 'transactiontimestamp',
                       'memberId': 'memberid',
                       'employeeId': 'employeeid',
                       'recurringServiceId': 'recurringserviceid',
                       'homeClub': 'homeclub',
                       'receiptNumber': 'receiptnumber',
                       'stationName': 'stationname',
                       'itemId': 'itemid',
                       'inventoryType': 'inventorytype',
                       'profitCenter': 'profitcenter',
                       'unitPrice': 'unitprice',
                       'paymentType': 'paymenttype',
                       'paymentAmount': 'paymentamount',
                       'paymentTax': 'paymenttax'}, inplace=True)
    return df


# summarize abc pos transactions
def summ_abc_pos_trans():
    summ_abc_pos_trans = """
                         insert into abc_pos_trans (employeeid,
                                                    etlrunid,
                                                    homeclub,
                                                    inventorytype,
                                                    itemid,
                                                    location,
                                                    memberid,
                                                    name,
                                                    profitcenter,
                                                    quantity,
                                                    receiptnumber,
                                                    recurringserviceid,
                                                    return_flag,
                                                    sale,
                                                    stationname,
                                                    subtotal,
                                                    tax,
                                                    transactionid,
                                                    transactiontimestamp,
                                                    unitprice,
                                                    upc)
                         SELECT employeeid, \
                                etlrunid, \
                                homeclub, \
                                inventorytype, \
                                itemid, \
                                location, \
                                memberid, \
                                name, \
                                -- sum(paymentamount) as paymentamount, \
                                -- sum(paymenttax) as paymenttax, \
                                -- max(paymenttype) as paymenttype	, \
                                profitcenter, \
                                quantity, \
                                receiptnumber, \
                                recurringserviceid, \
                                `return`, \
                                sale, \
                                stationname, \
                                subtotal, \
                                tax, \
                                transactionid, \
                                transactiontimestamp, \
                                unitprice, \
                                upc
                         FROM abc_transactions
                         where transactiontimestamp > \
                               (select coalesce(max(transactiontimestamp), '2020-01-01') from abc_pos_trans)
                         group by employeeid,
                                  etlrunid,
                                  homeclub,
                                  inventorytype,
                                  itemid,
                                  location,
                                  memberid,
                                  name,
                                  profitcenter,
                                  quantity,
                                  receiptnumber,
                                  recurringserviceid,
                                  `return`,
                                  sale,
                                  stationname,
                                  subtotal,
                                  tax,
                                  transactionid,
                                  transactiontimestamp,
                                  unitprice,
                                  upc \
                         """

    with engine.begin() as conn:
        conn.execute(text(summ_abc_pos_trans))

    return


# Function to create the 'transaction' table if it doesn't exist
#    needs to be updated and modified for various mysql / maria databases
#    also need to create an initialization job to check and create all the necessary tables
#    For scaling up we need to determine,
#        different dbs;
#        same db with row level security(performance);
#        same db, different tables & table space
def create_transaction_table_if_not_exists(connection):
    with connection.cursor() as cursor:
        create_table_query = """
                             CREATE TABLE `purchase_history` \
                             ( \
                                 `serviceName`         varchar(50) NOT NULL, \
                                 `serviceId`           varchar(50) NOT NULL, \
                                 `clubPurchased`       varchar(30) NOT NULL, \
                                 `purchaseDate`        timestamp, \
                                 `receiptNumber`       varchar(20) NOT NULL, \
                                 `totalPrice`          decimal(10, 2), \
                                 `expirationDate`      date, \
                                 `eventId`             varchar(30), \
                                 `eventName`           varchar(20), \
                                 `eventLevelId`        varchar(20), \
                                 `eventLevelName`      varchar(10), \
                                 `purchased`           int(4), \
                                 `unavailable`         int(4), \
                                 `scheduled`           int(4), \
                                 `available`           int(4), \
                                 `unscheduled`         int(4), \
                                 `serviceType`         varchar(20), \
                                 `deductible`          tinyint(1), \
                                 `availableForBooking` tinyint(1), \
                                 `mobile`              tinyint(1), \
                                 `memberid`            varchar(20)
                             ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci; \
                             """
        cursor.execute(create_table_query)
    connection.commit()


# Function to create the items data
def get_api_abc_club_items(url):
    # Make the request
    response = requests.get(url, headers=headers)
    resp_data = response.content.replace(b'\xc3\xa2\xc2\x80\xc2\x99', b'''''')

    flattened_df: DataFrame
    next_page: str = '0'

    # if the request was successful, parse and save to df
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        # Get the count and nextPage from the response
        status = data.get("status", {})
        count = int(status.get("count", 0))  # Convert count to integer
        next_page = status.get("nextPage")  # Get nextPage if available

        if count > 0:
            # Create DataFrame and flatten
            flattened_df = pd.json_normalize(data['items'])
        # print(flattened_df)
        return flattened_df, next_page
    else:
        print(f"Failed to retrieve data for ID {url}, Status Code: {response.status_code}")


# convert dataframe column types prior to writing to db
def convert_item_types(df):
    # Converting specific fields to the correct type

    df = df.replace({'Unlimited': '-1', r'[\$,]': ''}, regex=True)

    # Convert to correct types
    df['etlrunid'] = pd.to_numeric(df['etlrunid'], errors='coerce')
    df['club'] = pd.to_numeric(df['club'], errors='coerce')
    df['itemUnitPrice'] = pd.to_numeric(df['itemUnitPrice'], errors='coerce')
    df['inStock'] = pd.to_numeric(df['inStock'], errors='coerce')
    df['itemQuantity.minQuantity'] = pd.to_numeric(df['itemQuantity.minQuantity'], errors='coerce')
    df['itemQuantity.maxQuantity'] = pd.to_numeric(df['itemQuantity.maxQuantity'], errors='coerce')
    df['itemQuantity.defaultQuantity'] = pd.to_numeric(df['itemQuantity.defaultQuantity'], errors='coerce')

    # Boolean conversion
    # df['return'] = df['return'].astype(bool)
    # df['deductible'] = df['deductible'].map({'True': True, 'False': False})

    # Convert dates and timestamps
    # personal['birthDate'] = pd.to_datetime(personal.get('birthDate', None), errors='coerce')
    # df["expirationDate"] = df["expirationDate"].dt.date

    # Convert to numeric with coercion
    # df['A'] = pd.to_numeric(df['A'], errors='coerce')

    # Convert to numeric, handling NaN values
    # df['totalPrice'] = pd.to_numeric(df['totalPrice'], errors='coerce')

    # print(df.dtypes)

    # Convert NaN to None
    df = df.replace({np.nan: None, pd.NaT: None, 'Unlimited': '-1'})

    return df


# rename columns
def rename_item_columns(df):
    # rename columns, remove personal and shorten to be valid db column name
    df.rename(columns={'etlrunid': 'etlrunid',
                       'club': 'club',
                       'saleItemId': 'itemid',
                       'itemName': 'itemname',
                       'itemType': 'itemtype',
                       'productType': 'producttype',
                       'itemUnitPrice': 'itemunitprice',
                       'itemUpc': 'itemupc',
                       'inStock': 'instock',
                       'itemQuantity.minQuantity': 'minquantity',
                       'itemQuantity.maxQuantity': 'maxquantity',
                       'itemQuantity.defaultQuantity': 'defaultquantity',
                       'itemCategoryName': 'itemcategoryname',
                       'itemDescription': 'itemdescription'}, inplace=True)
    return df


# clear staging items
def clear_staging_items():
    # Empty staging table
    truncate_sql = """truncate staging.abc_items;"""

    with engine.begin() as conn:
        conn.execute(text(truncate_sql))

    return


# Upsert items
def upsert_items():
    # update the items data from staging
    # Perform UPSERT into target table
    # updat the rows and columns
    upsert_sql = """
                 INSERT INTO abc_items (etlrunid, club, itemid, itemname, itemtype, producttype, itemunitprice, itemupc, \
                                        instock, minquantity, maxquantity, defaultquantity, itemcategoryname, \
                                        itemdescription)
                 SELECT etlrunid, \
                        club, \
                        itemid, \
                        itemname, \
                        itemtype, \
                        producttype, \
                        itemunitprice, \
                        itemupc, \
                        instock, \
                        minquantity, \
                        maxquantity, \
                        defaultquantity, \
                        itemcategoryname, \
                        itemdescription \
                 FROM staging.abc_items ON DUPLICATE KEY \
                 UPDATE \
                     etlrunid = \
                 values (etlrunid), club = \
                 values (club), itemname = \
                 values (itemname), itemtype = \
                 values (itemtype), producttype = \
                 values (producttype), itemunitprice = \
                 values (itemunitprice), itemupc = \
                 values (itemupc), instock = \
                 values (instock), minquantity = \
                 values (minquantity), maxquantity = \
                 values (maxquantity), defaultquantity = \
                 values (defaultquantity), itemcategoryname = \
                 values (itemcategoryname), itemdescription = \
                 values (itemdescription); \
                 """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))

    return


# Function to create the 'items' table if it doesn't exist
#    needs to be updated and modified for various mysql / maria databases
#    also need to create an initialization job to check and create all the necessary tables
#    For scaling up we need to determine,
#        different dbs;
#        same db with row level security(performance);
#        same db, different tables & table space
def create_items_table_if_not_exists(connection):
    with connection.cursor() as cursor:
        create_table_query = """
                             CREATE TABLE `abc_items` \
                             ( \
                                 `etlrunid`      mediumint unsigned DEFAULT NULL, \
                                 `club`          mediumint    DEFAULT NULL, \
                                 `itemid`        varchar(100) DEFAULT NULL, \
                                 `itemname`      varchar(100) DEFAULT NULL, \
                                 `itemtype`      varchar(50)  DEFAULT NULL, \
                                 `producttype`   varchar(50)  DEFAULT NULL, \
                                 `itemunitprice` decimal(10.2 \
                             ) DEFAULT NULL,
              `itemupc` varchar(50) DEFAULT NULL,
              `instock` mediumint DEFAULT NULL,
              `minquantity` int DEFAULT NULL,
              `maxquantity` int DEFAULT NULL,
              `defaultquantity` int DEFAULT NULL,
              `itemcategoryname` varchar(100) DEFAULT NULL,
              `itemdescription` varchar(255) DEFAULT NULL,
              PRIMARY KEY (`itemid`)
            ); \
                             """
        cursor.execute(create_table_query)
    connection.commit()


# Function to create the item categories data
def get_api_abc_club_item_categories(url):
    # Make the request
    response = requests.get(url, headers=headers)
    resp_data = response.content.replace(b'\xc3\xa2\xc2\x80\xc2\x99', b'''''')

    flattened_df: DataFrame
    next_page: str = '0'

    # if the request was successful, parse and save to df
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        # Get the count and nextPage from the response
        status = data.get("status", {})
        count = int(status.get("count", 0))  # Convert count to integer
        next_page = status.get("nextPage")  # Get nextPage if available

        if count > 0:
            # Create DataFrame and flatten
            flattened_df = pd.json_normalize(data['itemCategories'])
        # print(flattened_df)
        return flattened_df, next_page
    else:
        print(f"Failed to retrieve data for ID {url}, Status Code: {response.status_code}")


# convert dataframe column types prior to writing to db
def convert_item_categories_types(df):
    # Converting specific fields to the correct type

    df = df.replace({'Unlimited': '-1', r'[\$,]': ''}, regex=True)

    # Convert to correct types
    df['etlrunid'] = pd.to_numeric(df['etlrunid'], errors='coerce')
    df['club'] = pd.to_numeric(df['club'], errors='coerce')
    df['displayInPos'] = df['displayInPos'].astype(bool)
    df['displayInRs'] = df['displayInRs'].astype(bool)

    # Boolean conversion
    # df['return'] = df['return'].astype(bool)
    # df['fieldname'] = df['fieldname'].str.lower() == 'true'  # true/True = 1, all other values 0
    # df['deductible'] = df['deductible'].map({'True': True, 'False': False})
    # df['fieldname'] = df['fieldname'].str.lower().map({'true': True, 'false': False})
    # df['active'] = df['active'].fillna(False)  # optional: treat unknowns as False
    # df['active'] = df['active'].astype(int)  # for MySQL TINYINT

    # Convert dates and timestamps
    # personal['birthDate'] = pd.to_datetime(personal.get('birthDate', None), errors='coerce')
    # df["expirationDate"] = df["expirationDate"].dt.date

    # Convert to numeric with coercion
    # df['A'] = pd.to_numeric(df['A'], errors='coerce')

    # Convert to numeric, handling NaN values
    # df['totalPrice'] = pd.to_numeric(df['totalPrice'], errors='coerce')

    # print(df.dtypes)

    # Convert NaN to None
    df = df.replace({np.nan: None, pd.NaT: None, 'Unlimited': '-1'})

    return df


# rename columns
def rename_item_categories_columns(df):
    # rename columns, remove personal and shorten to be valid db column name
    df.rename(columns={'etlrunid': 'etlrunid',
                       'club': 'club',
                       'itemCategoryId': 'itemcategoryid',
                       'itemCategoryName': 'itemcategoryname',
                       'itemCategoryDescription': 'itemcategorydescription',
                       'displayInPos': 'displayinpos',
                       'displayInRs': 'displayinrs'}, inplace=True)
    return df


# clear staging item categories
def clear_staging_item_categories():
    # Empty staging table
    truncate_sql = """truncate staging.abc_item_categories;"""

    with engine.begin() as conn:
        conn.execute(text(truncate_sql))

    return


# Upsert item categories
def upsert_item_categories():
    # update the item categories data from staging
    # Perform UPSERT into target table
    # updat the rows and columns
    upsert_sql = """
                 INSERT INTO abc_item_categories (etlrunid, club, itemcategoryid, itemcategoryname, \
                                                  itemcategorydescription, displayinpos, displayinrs)
                 SELECT etlrunid, \
                        club, \
                        itemcategoryid, \
                        itemcategoryname, \
                        itemcategorydescription, \
                        displayinpos, \
                        displayinrs \
                 FROM staging.abc_item_categories ON DUPLICATE KEY \
                 UPDATE \
                     etlrunid = \
                 values (etlrunid), club = \
                 values (club), itemcategoryname = \
                 values (itemcategoryname), itemcategorydescription = \
                 values (itemcategorydescription), displayinpos = \
                 values (displayinpos), displayinrs = \
                 values (displayinrs); \
                 """

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))

    return


# Function to get plans
def get_api_abc_club_plans(url):
    # Make the request
    response = requests.get(url, headers=headers)
    resp_data = response.content.replace(b'\xc3\xa2\xc2\x80\xc2\x99', b'''''')

    flattened_df: DataFrame
    next_page: str = '0'

    # if the request was successful, parse and save to df
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        # Get the count and nextPage from the response
        status = data.get("status", {})
        count = int(status.get("count", 0))  # Convert count to integer
        next_page = status.get("nextPage")  # Get nextPage if available

        if count > 0:
            # Create DataFrame and flatten
            flattened_df = pd.json_normalize(data['plans'])
        # print(flattened_df)
        return flattened_df, next_page
    else:
        print(f"Failed to retrieve data for ID {url}, Status Code: {response.status_code}")


# used in the next two routines
expected_plans_columns = ["etlrunid", "club", "planId", "planName", "promoCode", "promoName", "agreementDescription",
                          "limitedAvailability", "planStartDate", "planEndDate", "onlinePlanDisplayLocation",
                          "corporatePlanOnly", "additionalMembersAllowed", "mobilePaymentPlan", "corporatePlanOnly"]


# find missing and/or extra plan columns
def audit_df_plans_columns(df):
    missing = [col for col in expected_plans_columns if col not in df.columns]
    extra = [col for col in df.columns if col not in expected_plans_columns]
    return missing, extra


# the api returns a variable number of json fields, add necessary ones in db and remove extra not in db.
# we need a way to audit the extra ones to ensure that we update the routines to capture the missing data
def normalize_df_plans_columns(df):
    # first add any missing columns
    for col in expected_plans_columns:
        if col not in df.columns:
            df[col] = None
    # second remove extra columns
    df = df[expected_plans_columns]  # Only keep the expected ones
    return df


# convert plan dataframe column types prior to writing to db
def convert_plans_types(df):
    # Convert NaN to None & Unlimited to -1 since it came as data in an numeric field
    # df = df.replace({np.nan: None, pd.NaT: None, '\bUnlimited\b': '-1'})

    # Converting specific fields to the correct type
    df["planStartDate"] = pd.to_datetime(df["planStartDate"], errors='coerce')
    df["planEndDate"] = pd.to_datetime(df["planEndDate"], errors='coerce')
    df['limitedAvailability'] = df['limitedAvailability'].str.lower() == 'true'
    df['corporatePlanOnly'] = df['corporatePlanOnly'].str.lower() == 'true'
    df['additionalMembersAllowed'] = df['additionalMembersAllowed'].str.lower() == 'true'
    df['mobilePaymentPlan'] = df['mobilePaymentPlan'].str.lower() == 'true'

    # Boolean conversion
    # df['return'] = df['return'].astype(bool)
    # df['fieldname'] = df['fieldname'].str.lower() == 'true'  # true/True = 1, all other values 0
    # df['deductible'] = df['deductible'].map({'True': True, 'False': False})
    # df['fieldname'] = df['fieldname'].str.lower().map({'true': True, 'false': False})
    # df['active'] = df['active'].fillna(False)  # optional: treat unknowns as False
    # df['active'] = df['active'].astype(int)  # for MySQL TINYINT

    # Convert dates and timestamps
    # personal['birthDate'] = pd.to_datetime(personal.get('birthDate', None), errors='coerce')
    # df["expirationDate"] = df["expirationDate"].dt.date

    # Convert to numeric with coercion
    # df['A'] = pd.to_numeric(df['A'], errors='coerce')

    # Convert to numeric, handling NaN values
    # df['totalPrice'] = pd.to_numeric(df['totalPrice'], errors='coerce')

    # print(df.dtypes)

    # Convert NaN to None & Unlimited to -1 since it came as data in an numeric field
    # df = df.replace({np.nan: None, pd.NaT: None, '\bUnlimited\b': '-1'})

    return df


# rename plan columns
def rename_plans_columns(df):
    # rename columns, remove personal and shorten to be valid db column name
    df.rename(columns={'etlrunid': 'etlrunid',
                       'club': 'club',
                       'planId': 'planid',
                       'planName': 'planname',
                       'promoCode': 'promocode',
                       'promoName': 'promoname',
                       'agreementDescription': 'agreementdescription',
                       'limitedAvailability': 'limitedavailability',
                       'planStartDate': 'planstartdate',
                       'planEndDate': 'planenddate',
                       'additionalMembersAllowed': 'additionalmembersallowed',
                       'mobilePaymentPlan': 'mobilepaymentplan',
                       'onlinePlanDisplayLocation': 'onlineplandisplaylocation',
                       'corporatePlanOnly': 'corporateplanonly'}, inplace=True)
    return df


# clear staging plans
def clear_staging_plans():
    # Empty staging table
    truncate_sql = """truncate staging.abc_plans;"""

    with engine.begin() as conn:
        conn.execute(text(truncate_sql))

    return


# Upsert plans
def upsert_plans():
    # update the plans data from staging
    # Perform UPSERT into target table
    # update the rows and columns
    upsert_sql = """
                 INSERT INTO abc_plans (etlrunid, club, planid, planname, promocode, promoname, agreementdescription, \
                                        planstartdate, planenddate, onlineplandisplaylocation, additionalmembersallowed, \
                                        mobilepaymentplan, limitedavailability, corporateplanonly)
                 SELECT etlrunid, \
                        club, \
                        planid, \
                        planname, \
                        promocode, \
                        promoname, \
                        agreementdescription, \
                        planstartdate, \
                        planenddate, \
                        onlineplandisplaylocation, \
                        additionalmembersallowed, \
                        mobilepaymentplan, \
                        limitedavailability, \
                        corporateplanonly \
                 FROM staging.abc_plans ON DUPLICATE KEY \
                 UPDATE \
                     etlrunid = \
                 values (etlrunid), planname = \
                 values (planname), promocode = \
                 values (promocode), promoname = \
                 values (promoname), agreementdescription = \
                 values (agreementdescription), planstartdate = \
                 values (planstartdate), planenddate = \
                 values (planenddate), onlineplandisplaylocation = \
                 values (onlineplandisplaylocation), additionalmembersallowed = \
                 values (additionalmembersallowed), mobilepaymentplan = \
                 values (mobilepaymentplan), limitedavailability = \
                 values (limitedavailability), corporateplanonly = \
                 values (corporateplanonly); \
                 """

    # club = values(club),

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))

    return


# Function to get plan detail data
def get_api_abc_club_plan(url):
    # Make the request
    response = requests.get(url, headers=headers)
    resp_data = response.content.replace(b'\xc3\xa2\xc2\x80\xc2\x99', b'''''')

    flattened_df: DataFrame
    next_page: str = '0'

    # if the request was successful, parse and save to df
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        # Get the count and nextPage from the response
        status = data.get("status", {})
        count = int(status.get("count", 0))  # Convert count to integer
        next_page = status.get("nextPage")  # Get nextPage if available

        if count > 0:
            payment_plan = data.get("paymentPlan", [])
        else:
            payment_plan = []
        # print(payment_plan)

        # Normalize and flatten the structure: explode downpayments
        # Choose base fields to carry over to every record
        plan_base = {
            k: v for k, v in payment_plan.items() if not isinstance(v, list)
            # k: v for k, v in payment_plan.items() if not isinstance(v, list) and not isinstance(v, dict)
        }

        # plan_df1 = pd.json_normalize(payment_plan, sep="_")

        # Container for flattened records
        records = []

        # Expand downPayments
        cntr = 0
        for dp in payment_plan.get("downPayments", []):
            record = {**plan_base, **dp}
            cntr += 1
            record["recordType"] = "downPayment"
            record["downpaymentid"] = cntr
            records.append(record)

        # Convert to DataFrame
        flattened_df = pd.json_normalize(records, sep="_")  # pd.DataFrame(records)

        # -- failed attempt to use a loop, will retry at a later point
        # for txn in payment_plan:
        #     txn_base = {k: v for k, v in txn.downPayment() if k != "downPayment"}
        #     down_payment = txn.get("downPayment", {}).get("downPayment", [])
        # records = {
        #     **txn_base,
        #     **down_payment
        # }

        # -- loop from list of transations that have multiple items that each have multiple payments
        # # Normalize and flatten the structure: explode items and payments
        # records = []
        # for txn in purchase_hist:
        #     txn_base = {k: v for k, v in txn.items() if k != "items"}
        #     items = txn.get("items", {}).get("item", [])
        #     for item in items:
        #         item_base = {k: v for k, v in item.items() if k != "payments"}
        #         payments = item.get("payments", [])
        #         for payment in payments:
        #             # Combine transaction, item, and payment data
        #             record = {
        #                 **txn_base,
        #                 **item_base,
        #                 **payment
        #             }
        #             records.append(record)

        # if count > 0:
        #     # Create DataFrame and flatten
        #
        #     # Extract the plan
        #     plan = data.get("paymentPlan", {})
        #
        #     # Choose base fields to carry over to every record
        #     plan_base = {
        #         k: v for k, v in plan.items() if not isinstance(v, list) and not isinstance(v, dict)
        #     }
        #
        #     # Container for flattened records
        #     records = []
        #
        #     # Expand downPayments
        #     for dp in plan.get("downPayments", []):
        #         record = {**plan_base, **dp}
        #         record["recordType"] = "downPayment"
        #         records.append(record)
        #
        #     # Expand schedules
        #     for sch in plan.get("schedules", []):
        #         record = {**plan_base, **sch}
        #         record["recordType"] = "schedule"
        #         records.append(record)
        #
        #     # Expand userDefinedFields the same way
        #     for udf in plan.get("userDefinedFields", []):
        #         record = {**plan_base, **udf}
        #         record["recordType"] = "userDefinedField"
        #         records.append(record)
        #
        #     # # Expand  fieldOption the same way -- not sure if needed
        #     # for fo in plan.get("fieldOption", []):
        #     #     record = {**plan_base, **fo}
        #     #     record["recordType"] = "fieldOption"
        #     #     records.append(record)
        #
        #     # Convert to DataFrame
        #     flattened_df = pd.DataFrame(records)

        # print(flattened_df)
        return flattened_df, next_page
    else:
        print(f"Failed to retrieve data for ID {url}, Status Code: {response.status_code}")


# used in the next two routines
# may need to add more columns
expected_plan_columns = ["planName", "planId", "active", "promotionCode", "promotionName", "membershipType",
                         "agreementTerm",
                         "scheduleFrequency", "termInMonths", "dueDay", "firstDueDate", "activePresale",
                         "expirationDate",
                         "onlineSignupAllowedPaymentMethods", "preferredPaymentMethod", "totalContractValue",
                         "downPaymentName",
                         "downPaymentTotalAmount", "scheduleTotalAmount", "agreementTerms", "agreementDescription",
                         "agreementNote",
                         "clubFeeTotalAmount", "planValidation", "onlinePlanDisplayLocation", "corporatePlanOnly",
                         "additionalMembersAllowed",
                         "renewalInformation_term", "renewalInformation_paymentAmount",
                         "renewalInformation_renewalFrequency", "name", "subTotal",
                         "tax", "total", "recordType", "downpaymentid", "club", "etlrunid"]


# find missing and/or extra plan columns
def audit_df_plan_columns(df):
    missing = [col for col in expected_plan_columns if col not in df.columns]
    extra = [col for col in df.columns if col not in expected_plan_columns]
    return missing, extra


# the api returns a variable number of json fields, add necessary ones in db and remove extra not in db.
# we need a way to audit the extra ones to ensure that we update the routines to capture the missing data
def normalize_df_plan_columns(df):
    # first add any missing columns
    for col in expected_plan_columns:
        if col not in df.columns:
            df[col] = None
    # second remove extra columns
    df = df[expected_plan_columns]  # Only keep the expected ones
    return df


# convert plan dataframe column types prior to writing to db
def convert_plan_types(df):
    # Convert NaN to None & Unlimited to -1 since it came as data in an numeric field
    # df = df.replace({np.nan: None, pd.NaT: None, '\bUnlimited\b': '-1'})

    df = df.replace(r'[$,]', '', regex=True)
    # str.replace(r'[$,]', '', regex=True)
    # Converting specific fields to the correct type
    # may need to add club and etlrunid, plan validation very large number-ensure correct in db
    df['termInMonths'] = pd.to_numeric(df['termInMonths'], errors='coerce')
    df['dueDay'] = pd.to_numeric(df['dueDay'], errors='coerce')
    df['totalContractValue'] = pd.to_numeric(df['totalContractValue'], errors='coerce')
    df['downPaymentTotalAmount'] = pd.to_numeric(df['downPaymentTotalAmount'], errors='coerce')
    df['scheduleTotalAmount'] = pd.to_numeric(df['scheduleTotalAmount'], errors='coerce')
    df['clubFeeTotalAmount'] = pd.to_numeric(df['clubFeeTotalAmount'], errors='coerce')
    df['planValidation'] = pd.to_numeric(df['planValidation'], errors='coerce')
    df['additionalMembersAllowed'] = pd.to_numeric(df['additionalMembersAllowed'], errors='coerce')
    df['subTotal'] = pd.to_numeric(df['subTotal'], errors='coerce')
    df['tax'] = pd.to_numeric(df['tax'], errors='coerce')
    df['total'] = pd.to_numeric(df['total'], errors='coerce')
    df['renewalInformation_paymentAmount'] = pd.to_numeric(df['renewalInformation_paymentAmount'], errors='coerce')
    df['downpaymentid'] = pd.to_numeric(df['downpaymentid'], errors='coerce')

    # may need to convert these to date prior to writing to db
    df["firstDueDate"] = pd.to_datetime(df["firstDueDate"], errors='coerce')
    df["expirationDate"] = pd.to_datetime(df["expirationDate"], errors='coerce')

    df['active'] = df['active'].astype(str).str.lower() == 'true'
    df['activePresale'] = df['activePresale'].astype(str).str.lower() == 'true'
    df['corporatePlanOnly'] = df['corporatePlanOnly'].astype(str).str.lower() == 'true'

    # Boolean conversion
    # df['return'] = df['return'].astype(bool)
    # df['fieldname'] = df['fieldname'].astype(str).str.lower() == 'true'  # true/True = 1, all other values 0
    # df['deductible'] = df['deductible'].map({'True': True, 'False': False})
    # df['fieldname'] = df['fieldname'].str.lower().map({'true': True, 'false': False})
    # df['active'] = df['active'].fillna(False)  # optional: treat unknowns as False
    # df['active'] = df['active'].astype(int)  # for MySQL TINYINT

    # Convert dates and timestamps
    # personal['birthDate'] = pd.to_datetime(personal.get('birthDate', None), errors='coerce')
    # df["expirationDate"] = df["expirationDate"].dt.date

    # Convert to numeric with coercion
    # Convert to numeric, handling NaN values
    # df['totalPrice'] = pd.to_numeric(df['totalPrice'], errors='coerce')

    # print(df.dtypes)

    # Convert NaN to None & Unlimited to -1 since it came as data in an numeric field
    # df = df.replace({np.nan: None, pd.NaT: None, '\bUnlimited\b': '-1'})

    # in this data set plan type is Unlimited, so we dont want to change that.
    df = df.replace({np.nan: None, pd.NaT: None})

    return df


# rename plan columns
def rename_plan_columns(df):
    # rename columns, remove personal and shorten to be valid db column name
    df.rename(columns={'planName': 'planname',
                       'planId': 'planid',
                       'active': 'activestatus',
                       'promotionCode': 'promotioncode',
                       'promotionName': 'promotionname',
                       'membershipType': 'membershiptype',
                       'agreementTerm': 'agreementterm',
                       'scheduleFrequency': 'schedulefrequency',
                       'termInMonths': 'terminmonths',
                       'dueDay': 'dueday',
                       'firstDueDate': 'firstduedate',
                       'activePresale': 'activepresale',
                       'expirationDate': 'expirationdate',
                       'onlineSignupAllowedPaymentMethods': 'onlinesignupallowedpaymentmethods',
                       'preferredPaymentMethod': 'preferredpaymentmethod',
                       'totalContractValue': 'totalcontractvalue',
                       'downPaymentName': 'downpaymentname',
                       'downPaymentTotalAmount': 'downpaymenttotalamount',
                       'scheduleTotalAmount': 'scheduletotalamount',
                       'agreementTerms': 'agreementterms',
                       'agreementDescription': 'agreementdescription',
                       'agreementNote': 'agreementnote',
                       'clubFeeTotalAmount': 'clubfeetotalamount',
                       'planValidation': 'planvalidation',
                       'onlinePlanDisplayLocation': 'onlineplandisplaylocation',
                       'corporatePlanOnly': 'corporateplanonly',
                       'additionalMembersAllowed': 'additionalmembersallowed',
                       'renewalInformation_term': 'renewalinformation_term',
                       'renewalInformation_paymentAmount': 'renewalinformation_paymentamount',
                       'renewalInformation_renewalFrequency': 'renewalinformation_renewalfrequency',
                       'name': 'itemname',
                       'subTotal': 'subtotal',
                       'tax': 'tax',
                       'total': 'total',
                       'downpaymentid': 'downpaymentid',
                       'recordType': 'recordtype',
                       'club': 'club',
                       'etlrunid': 'etlrunid'}, inplace=True)
    return df


# clear staging plans
def clear_staging_plan():
    # Empty staging table
    truncate_sql = """truncate staging.abc_plan;"""

    with engine.begin() as conn:
        conn.execute(text(truncate_sql))

    return


# Upsert plans
def upsert_plan():
    # update the plans data from staging
    # Perform UPSERT into target table
    # update the rows and columns
    upsert_sql = """
                 INSERT INTO abc_plan (planname, planid, activestatus, promotioncode, promotionname, membershiptype, \
                                       agreementterm, schedulefrequency, terminmonths, dueday, firstduedate, \
                                       activepresale, expirationdate, onlinesignupallowedpaymentmethods, \
                                       preferredpaymentmethod, totalcontractvalue, downpaymentname, \
                                       downpaymenttotalamount, scheduletotalamount, agreementterms, \
                                       agreementdescription, agreementnote, clubfeetotalamount, planvalidation, \
                                       onlineplandisplaylocation, corporateplanonly, additionalmembersallowed, \
                                       renewalinformation_term, renewalinformation_paymentamount, \
                                       renewalinformation_renewalfrequency, itemname, subtotal, tax, total, \
                                       downpaymentid, recordtype, club, etlrunid)
                 SELECT planname, \
                        planid, \
                        activestatus, \
                        promotioncode, \
                        promotionname, \
                        membershiptype, \
                        agreementterm, \
                        schedulefrequency, \
                        terminmonths, \
                        dueday, \
                        firstduedate, \
                        activepresale, \
                        expirationdate, \
                        onlinesignupallowedpaymentmethods, \
                        preferredpaymentmethod, \
                        totalcontractvalue, \
                        downpaymentname, \
                        downpaymenttotalamount, \
                        scheduletotalamount, \
                        agreementterms, \
                        agreementdescription, \
                        agreementnote, \
                        clubfeetotalamount, \
                        planvalidation, \
                        onlineplandisplaylocation, \
                        corporateplanonly, \
                        additionalmembersallowed, \
                        renewalinformation_term, \
                        renewalinformation_paymentamount, \
                        renewalinformation_renewalfrequency, \
                        itemname, \
                        subtotal, \
                        tax, \
                        total, \
                        downpaymentid, \
                        recordtype, \
                        club, \
                        etlrunid
                 FROM staging.abc_plan ON DUPLICATE KEY \
                 UPDATE \
                     planname = \
                 values (planname), activestatus = \
                 values (activestatus), promotioncode = \
                 values (promotioncode), promotionname = \
                 values (promotionname), membershiptype = \
                 values (membershiptype), agreementterm = \
                 values (agreementterm), schedulefrequency = \
                 values (schedulefrequency), terminmonths = \
                 values (terminmonths), dueday = \
                 values (dueday), firstduedate = \
                 values (firstduedate), activepresale = \
                 values (activepresale), expirationdate = \
                 values (expirationdate), onlinesignupallowedpaymentmethods = \
                 values (onlinesignupallowedpaymentmethods), preferredpaymentmethod = \
                 values (preferredpaymentmethod), totalcontractvalue = \
                 values (totalcontractvalue), downpaymentname = \
                 values (downpaymentname), downpaymenttotalamount = \
                 values (downpaymenttotalamount), scheduletotalamount = \
                 values (scheduletotalamount), agreementterms = \
                 values (agreementterms), agreementdescription = \
                 values (agreementdescription), agreementnote = \
                 values (agreementnote), clubfeetotalamount = \
                 values (clubfeetotalamount), planvalidation = \
                 values (planvalidation), onlineplandisplaylocation = \
                 values (onlineplandisplaylocation), corporateplanonly = \
                 values (corporateplanonly), additionalmembersallowed = \
                 values (additionalmembersallowed), renewalinformation_term = \
                 values (renewalinformation_term), renewalinformation_paymentamount = \
                 values (renewalinformation_paymentamount), renewalinformation_renewalfrequency = \
                 values (renewalinformation_renewalfrequency), itemname = \
                 values (itemname), subtotal = \
                 values (subtotal), tax = \
                 values (tax), total = \
                 values (total), recordtype = \
                 values (recordtype), etlrunid = \
                 values (etlrunid); \
                 """

    # club = values(club),

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))

    return


##########################################################################################################################

# Function to create the campaigns data
def get_api_abc_club_campaigns(url):
    # Make the request
    response = requests.get(url, headers=headers)
    resp_data = response.content.replace(b'\xc3\xa2\xc2\x80\xc2\x99', b'''''')

    flattened_df: DataFrame
    next_page: str = '0'

    # if the request was successful, parse and save to df
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        # Get the count and nextPage from the response
        status = data.get("status", {})
        count = int(status.get("count", 0))  # Convert count to integer
        next_page = status.get("nextPage")  # Get nextPage if available

        if count > 0:
            # Create DataFrame and flatten
            flattened_df = pd.json_normalize(data['campaigns'])
        # print(flattened_df)

        # Check list of dataframe columns
        column_list = flattened_df.columns.values.tolist()
        print(column_list)

        return flattened_df, next_page
    else:
        print(f"Failed to retrieve data for ID {url}, Status Code: {response.status_code}")


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