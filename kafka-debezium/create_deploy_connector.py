#import json
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
CONNECTOR_NAME = "banking-postgres-connector"

MAX_RETRIES = 10
WAIT_SECONDS = 10

connector_config = {
    "name": CONNECTOR_NAME,
    "config": {

        # -----------------------------
        # Connector
        # -----------------------------
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # -----------------------------
        # PostgreSQL
        # -----------------------------
        "database.hostname": os.getenv("KAFKA_POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),

        # -----------------------------
        # Topic
        # -----------------------------
        "topic.prefix": "banking",

        # -----------------------------
        # Capture only required tables
        # -----------------------------
        "table.include.list":
            "public.customers,"
            "public.accounts,"
            "public.transactions",
        # -----------------------------
        # PostgreSQL logical replication
        # -----------------------------
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.name": "banking_publication",
        "publication.autocreate.mode": "filtered",

        # -----------------------------
        # Snapshot
        # -----------------------------
        "snapshot.mode": "initial",
        "snapshot.locking.mode": "none",

        # -----------------------------
        # Data handling
        # -----------------------------
        "decimal.handling.mode": "double",
        "tombstones.on.delete": "false",

        "slot.drop.on.stop": "false",
        "heartbeat.interval.ms": "10000",

        # -----------------------------
        # Kafka Connect JSON converters
        # -----------------------------
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false"
    }
}


upd_url = f"http://localhost:8083/connectors/{CONNECTOR_NAME}/config"
# -----------------------------
# Delete existing connector if it exists
# -----------------------------
# delete_url = f"http://localhost:8083/connectors/{CONNECTOR_NAME}"
# try:
#     print("Checking for existing connector to remove...")
#     del_response = requests.delete(delete_url)
#     if del_response.status_code == 204:
#         print("🗑️ Old connector deleted successfully.")
#         time.sleep(2) # Give Kafka Connect a moment to clean up
# except requests.exceptions.ConnectionError:
#     pass


# New Connector

def deploy_connector():
    url = f"{CONNECT_URL}/connectors"
    headers = { "Content-Type": "application/json" }

    print("Waiting for Kafka Connect...")

    for attempt in range(MAX_RETRIES):
        try:
            existing = requests.get(f"{url}/{CONNECTOR_NAME}")
            if existing.status_code == 200:
                print(f"Connector '{CONNECTOR_NAME}' already exists.")
                return
            
            response = requests.post(url, headers=headers, json=connector_config, timeout=10 )
        
            # for changing configs : Change method to PUT, and pass ONLY the "config" dictionary as the payload
            #response = requests.put(upd_url, headers=headers, json=connector_config["config"])

            if response.status_code == 201:
                print("Connector created and deployed successfully.")
                return

            elif response.status_code == 409:
                print(f"Connector '{CONNECTOR_NAME}' already exists.")
                return

            else:
                print(f"❌ Failed to create connector ({response.status_code}): {response.text}")
                return

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            print(f"Attempt {attempt+1}/{MAX_RETRIES}")
            time.sleep(WAIT_SECONDS)

    print("Kafka Connect unavailable.")


if __name__ == "__main__":
    deploy_connector()