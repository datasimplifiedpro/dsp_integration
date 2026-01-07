# app_config.py
import os
import sys
from dotenv import load_dotenv
import asyncio
from onepassword.client import Client
from get_secret_utils import get_1p_secret

#       Calling Prog Envi    job    seq
#       ------------ ------- ------ ---
# usage:  program.py                     Results  len: 1    Envi: TEST       Jobname: ''        SqlSeq: -1
# usage:  program.py preprod             Results  len: 2    Envi: preprod    Jobname: ''        SqlSeq: -1
# usage:  program.py preprod de_dup      Results  len: 3    Envi: TEST       Jobname: 'dedup'   SqlSeq: -1
# usage:  program.py preprod de_dup 40   Results  len: 4    Envi: TEST       Jobname: ''        SqlSeq: -1

# Load environment variables from .env file
load_dotenv()

ONEP_HEADER = {
    "auth": os.getenv('1P_TOKEN'),
    "integration_name": os.getenv('1P_INT_NAME'),
    "integration_version": os.getenv('1P_INT_VERSION')
}

# initialize vars
jobname = '' # if not blank, fetch jobseq using jobname
sqlseq = -1  # if eq -1 then exectute all items in jobseq in order, if not eq -1 then only execute this sqlseq for the jobseq

environment = sys.argv[1] if len(sys.argv) > 1 else "TEST" # "DEV"
if len(sys.argv) > 2:
    jobname = sys.argv[2]
if len(sys.argv) > 3:
    sqlseq = sys.argv[3]

print(f"Running in environment: {environment}")

#temporary for testing, eventually we will pull the ids from vw_integration (line 72)
vaultid_test = "tutnr7sl57s35f7e6pmzer2tuy"
itemid_test = "djpqkjqlrhc3bptyvtgl7v2z7m"

vaultid_prod = "tutnr7sl57s35f7e6pmzer2tuy"
itemid_prod = "qxlbvydemo345ewg7xdszyth4u"

creds_df_TEST = asyncio.run(get_1p_secret(vaultid_test, itemid_test))
creds_df_PROD = asyncio.run(get_1p_secret(vaultid_prod, itemid_prod))

creds_by_env = {
    "TEST": creds_df_TEST,
    "PROD": creds_df_PROD
}

# Dynamically select the credentials based on environment
creds = creds_by_env[environment]

DB_CONFIG = {
    "host": creds.get('server'),
    "port": int(creds.get('port')),
    "database": creds.get('database'),
    "user": creds.get('username'),
    "password": creds.get('password')
}

# def get_db_integration():
#     try:
#         df = pd.read_sql("select * from vw_integration", con=engine)
#     except Error as e:
#         print("Error reading vw_integration:", e)
#     return df

# # list of Club IDs to substitute into the API URL
# integration_df = get_db_integration()
#
# vaultid = integration_df['vault_id'].iloc[0]
# itemid = integration_df['item_id'].iloc[0]



# host_env = "DB_HOST_" + environment
# port_env = "DB_PORT_" + environment
# database_env = "DB_NAME_" + environment
# user_env = "DB_USER_" + environment
# password_env = "DB_PASSWORD_" + environment
#
# DB_CONFIG = {
#     "host": os.getenv(host_env, "localhost"),
#     "port": int(os.getenv(port_env, 3306)),
#     "database": os.getenv(database_env, "vidamemb"),
#     "user": os.getenv(user_env, "root"),
#     "password": os.getenv(password_env, "<PASSWORD>"),
# }



# Zenoti keys for the request
ZENOTI_API_KEYS = {
    "USA": os.getenv("ZENOTI_API_KEY"),
    "Canada": os.getenv("ZENOTI_API_KEY_CAN")
}


# HubSpot header for the request
HUBSPOT_API_HEADER = {
    "Authorization": f"Bearer {os.getenv('HUBSPOT_API_KEY')}",
    "Content-Type": "application/json"
}


print("name:", __name__)

def main():
    print(f"Environment= {environment}")

if __name__ == "__main__":
   main()