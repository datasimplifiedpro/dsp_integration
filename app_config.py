# app_config.py
import os
import sys
from dotenv import load_dotenv
import asyncio
from onepassword.client import Client

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


# Headers for the request
ABC_API_HEADER = {
    "Accept": "application/json;charset=UTF-8",
    "app_id": os.getenv("ABC_APP_ID"),
    "app_key": os.getenv("ABC_APP_KEY")
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

async def get_1p_secret(vault_id, item_id):
    # Authenticate with service account token if provided

    client = await Client.authenticate(**ONEP_HEADER)

    # Get full item with fields
    full_item = await client.items.get(vault_id, item_id)

    # Return all fields as dictionary
    return {field.title: field.value for field in full_item.fields}

vaultid_env = "VAULTID_" + environment
itemid_env = "ITEMID_" + environment

vaultid = os.getenv(vaultid_env)
itemid = os.getenv(itemid_env)

creds = asyncio.run(get_1p_secret(vaultid, itemid))

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





print("name:", __name__)

def main():
    print(f"Environment= {environment}")

if __name__ == "__main__":
   main()