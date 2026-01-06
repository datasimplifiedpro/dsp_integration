# app_config.py
import os
import sys
from dotenv import load_dotenv

#       Calling Prog Envi    job    seq
#       ------------ ------- ------ ---
# usage:  program.py                     Results  len: 1    Envi: TEST       Jobname: ''        SqlSeq: -1
# usage:  program.py preprod             Results  len: 2    Envi: preprod    Jobname: ''        SqlSeq: -1
# usage:  program.py preprod de_dup      Results  len: 3    Envi: TEST       Jobname: 'dedup'   SqlSeq: -1
# usage:  program.py preprod de_dup 40   Results  len: 4    Envi: TEST       Jobname: ''        SqlSeq: -1

# Load environment variables from .env file
load_dotenv()


# initialize vars
jobname = '' # if not blank, fetch jobseq using jobname
sqlseq = -1  # if eq -1 then exectute all items in jobseq in order, if not eq -1 then only execute this sqlseq for the jobseq

environment = sys.argv[1] if len(sys.argv) > 1 else "TEST" # "DEV"
if len(sys.argv) > 2:
    jobname = sys.argv[2]
if len(sys.argv) > 3:
    sqlseq = sys.argv[3]

print(f"Running in environment: {environment}")

host_env = "DB_HOST_" + environment
port_env = "DB_PORT_" + environment
database_env = "DB_NAME_" + environment
user_env = "DB_USER_" + environment
password_env = "DB_PASSWORD_" + environment

DB_CONFIG = {
    "host": os.getenv(host_env, "localhost"),
    "port": int(os.getenv(port_env, 3306)),
    "database": os.getenv(database_env, "vidamemb"),
    "user": os.getenv(user_env, "root"),
    "password": os.getenv(password_env, "<PASSWORD>"),
}


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