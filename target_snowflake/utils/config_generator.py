#!/usr/bin/ python3
from datetime import datetime, timedelta
import json
import logging
from os import environ
import sys
from typing import Dict

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logging.info("Generating keyfile...")

CONFIG_DICT = environ.copy()
OUTPUT_FILE = "config.json"

# Get the secret credentials from env vars
keyfile_mapping = {
    "account": "SF_ACCOUNT",
    "username": "SF_USER",
    "password": "SF_PASSWORD",
    "role": "SF_ROLE",
    "database": "SF_DATABASE",
    "schema": "SF_TEST_SCHEMA",
    "warehouse": "SF_WAREHOUSE",
}
keyfile_dict = {k: CONFIG_DICT.get(v) for k, v in keyfile_mapping.items()}

if not keyfile_dict["account"]:
    raise ValueError(
        "The environment variable SF_ACCOUNT must hold your snowflake account."
    )
if "snowflakecomputing.com" in keyfile_dict["account"]:
    raise ValueError(
        "SF_ACCOUNT must *ONLY* contain your snowflake account, not the fully qualified domain name!"
    )

with open(OUTPUT_FILE, "w") as keyfile:
    json.dump(keyfile_dict, keyfile, sort_keys=True, indent=2)
    logging.info("Keyfile written to {} successfully.".format(OUTPUT_FILE))
