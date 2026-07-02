import os
import random
import time

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dotenv import load_dotenv

load_dotenv(override=True)

# Initialize Faker
fake = Faker()

# ---------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------

DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "banking")

# ---------------------------------------------------------------------
# Simulation Configuration
# ---------------------------------------------------------------------

INITIAL_CUSTOMERS = 20
SIMULATION = {"new_customer": 15, "new_account": 20, "transaction": 65}
ACCOUNT_TYPES = ["Checking", "Savings"]
TRANSACTION_TYPES = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]

# ---------------------------------------------------------------------
# Database Connection
# ---------------------------------------------------------------------

def get_connection():
    """Establish and return a database connection."""
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        conn.autocommit = True # We want transactions to commit immediately for CDC
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        exit(1)


# ---------------------------------------------------------------------
# Initial Seed
# ---------------------------------------------------------------------

def seed_initial_data(cursor, num_customers=20):
    print(f"Seeding {num_customers} customers...")
    customers = []
    for _ in range(num_customers):
        customers.append((
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number()[:20]
        ))

    execute_values(
        cursor,
        "INSERT INTO customers(first_name,last_name,email,phone) VALUES %s RETURNING customer_id;",
        customers
    )
    customer_ids = [r[0] for r in cursor.fetchall()]

    accounts = []
    for customer_id in customer_ids:
        for _ in range(random.randint(1, 2)):
            accounts.append((
                    customer_id,
                    random.choice(ACCOUNT_TYPES),   
                    round(random.uniform(1000, 5000), 2)
            ))

    execute_values(
        cursor,
        "INSERT INTO accounts(customer_id,account_type,balance) VALUES %s;",
        accounts
    )
    print("Initial seed completed.\n")


# ---------------------------------------------------------------------
# Customer Creation
# ---------------------------------------------------------------------

def create_customer(conn, cursor):
    try:
        cursor.execute(
            "INSERT INTO customers(first_name,last_name,email,phone) VALUES (%s,%s,%s,%s) RETURNING customer_id;",
            (
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number()[:20]
            )
        )
        customer_id = cursor.fetchone()[0]

        number_of_accounts = random.randint(1, 2)
        account_ids = []
        for _ in range(number_of_accounts):
            cursor.execute(
                "INSERT INTO accounts(customer_id,account_type,balance) VALUES (%s,%s,%s) RETURNING account_id;",
                (customer_id, random.choice(ACCOUNT_TYPES), round(random.uniform(1000, 5000), 2))
            )
            account_ids.append(cursor.fetchone()[0])

        print(
            f"[NEW CUSTOMER] Customer={customer_id} "
            f"Accounts={len(account_ids)}"
        )
    except psycopg2.Error as e:
            print(f"Customer Creation failed: {e}")
            conn.rollback()


# ---------------------------------------------------------------------
# Account Creation
# ---------------------------------------------------------------------

def create_account(conn, cursor):
    try:
        cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1;")
        row = cursor.fetchone()

        if row is None:
            return

        customer_id = row[0]

        cursor.execute(
            "INSERT INTO accounts(customer_id,account_type,balance) VALUES (%s,%s,%s) RETURNING account_id;",
            (customer_id, random.choice(ACCOUNT_TYPES),round(random.uniform(1000, 5000), 2))
        )
        account_id = cursor.fetchone()[0]
        print(
            f"[NEW ACCOUNT] Customer={customer_id} "
            f"Account={account_id}"
        )
    except psycopg2.Error as e:
            print(f"Account Creation failed: {e}")
            conn.rollback()


# ---------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------

def process_transaction(conn, cursor):
    try: 
        # Fetch random accounts to act upon
        cursor.execute("SELECT account_id,balance FROM accounts ORDER BY RANDOM() LIMIT 2;")
        rows = cursor.fetchall()

        if len(rows) < 2:
            return

        source_account = rows[0][0]
        source_balance = float(rows[0][1])
        destination_account = rows[1][0]
        transaction_type = random.choice(TRANSACTION_TYPES)
        amount = round(random.uniform(10, 500), 2)

        # Logic to respect database constraints (CHECK balance >= 0)
        if transaction_type in ("WITHDRAWAL", "TRANSFER") and source_balance < amount:
            transaction_type = "DEPOSIT"                # Flip to deposit if funds are insufficient


        # ---------------- Deposit ----------------
        if transaction_type == "DEPOSIT":
            cursor.execute(
                "UPDATE accounts SET balance = balance + %s,  updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;",
                (amount, source_account)
            )
            cursor.execute(
                "INSERT INTO transactions(account_id,transaction_type,amount) VALUES (%s,%s,%s);",
                (source_account, transaction_type, amount)
            )
            print(f"--> DEPOSIT: +${amount:.2f} to Account {source_account}")
        

        # ---------------- Withdrawal ----------------
        elif transaction_type == "WITHDRAWAL":
            cursor.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;",
                (amount, source_account)
            )
            cursor.execute(
                "INSERT INTO transactions(account_id,transaction_type,amount) VALUES (%s,%s,%s);",
                (source_account, transaction_type, amount)
            )
            print(f"<-- WITHDRAWAL: -${amount:.2f} from Account {source_account}")


        # ---------------- Transfer ----------------
        elif transaction_type == "TRANSFER":
            cursor.execute(
                "UPDATE accounts SET balance = balance - %s, updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;",
                (amount, source_account)
            )
            cursor.execute(
                "UPDATE accounts SET balance = balance + %s, updated_at = CURRENT_TIMESTAMP WHERE account_id = %s;",
                (amount,destination_account)
            )
            cursor.execute(
                "INSERT INTO transactions(account_id, transaction_type, amount, related_account_id) VALUES (%s,%s,%s,%s);",
                (source_account, transaction_type, amount, destination_account)
            )
            print(f"<-> TRANSFER: ${amount:.2f} from Account {source_account} to Account {destination_account}")

    except psycopg2.Error as e:
        print(f"Transaction failed: {e}")
        conn.rollback()

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    conn = get_connection()
    cursor = conn.cursor()

    # Check if database is already seeded
    cursor.execute("SELECT COUNT(*) FROM customers;")
    if cursor.fetchone()[0] == 0:
        seed_initial_data(cursor, INITIAL_CUSTOMERS)

    print("Starting Banking Simulator...\n")
    try:
        while True:
            try:
                action = random.choices(list(SIMULATION.keys()), weights=SIMULATION.values(), k=1)[0]

                if action == "new_customer":
                    create_customer(conn,cursor)

                elif action == "new_account":
                    create_account(conn,cursor)

                else:
                    process_transaction(conn,cursor)
                
            except Exception as e:
                print(f"Iteration Failed: {e}")

            time.sleep(random.uniform(1.5, 2.5))

    except KeyboardInterrupt:
        print("\nStopping simulator...")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()