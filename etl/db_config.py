import os
from dotenv import load_dotenv


ENV_PATH = "/opt/retail_project/.env"
if os.path.exists(ENV_PATH):
    # Running inside Airflow/Docker → use mounted path
    load_dotenv(ENV_PATH)
else:
    # Running locally → use current folder
    load_dotenv()

# REQUIRED VARIABLES CHECK
REQUIRED = ["DB_USER", "DB_PASSWORD", "DB_NAME",
            "DB_HOST_LOCAL", "DB_HOST_DOCKER"]
missing = [var_name for var_name in REQUIRED if not os.getenv(var_name)]

if missing:
    raise EnvironmentError(f"Missing required env vars: {missing}")

# AUTO-SELECT HOST
if os.path.exists("/.dockerenv"):
    ACTUAL_HOST = os.getenv("DB_HOST_DOCKER")   # = retail_postgres
else:
    ACTUAL_HOST = os.getenv("DB_HOST_LOCAL")    # = localhost


# FINAL CONFIG
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": ACTUAL_HOST,
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME")
}
