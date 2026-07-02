-- Active: 1782989500075@@127.0.0.1@5432@banking

-- Banking OLTP Initial Schema (Optimized for CDC)

-- Create Customers Table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create Accounts Table
CREATE TABLE IF NOT EXISTS accounts (
    account_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers (customer_id) ON DELETE CASCADE,
    account_type VARCHAR(20) NOT NULL,
    balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00 CHECK (balance >= 0),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    account_status VARCHAR(20) DEFAULT 'Active',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid (),
    account_id INT NOT NULL REFERENCES accounts (account_id) ON DELETE CASCADE,
    transaction_type VARCHAR(20) NOT NULL, -- DEPOSIT | WITHDRAWAL | TRANSFER
    amount DECIMAL(15, 2) NOT NULL CHECK (amount > 0),
    related_account_id INT NULL REFERENCES accounts (account_id) ON DELETE SET NULL, -- Enforced FK for transfers
    transaction_status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
    transaction_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Simple indexed columns for performance in queries (Fixed column name)
CREATE INDEX IF NOT EXISTS idx_transactions_account_date ON transactions (account_id, transaction_date);