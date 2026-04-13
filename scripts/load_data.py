"""Load UCI Online Retail dataset into PostgreSQL.

Downloads the dataset, normalizes it into 4 tables
(customers, products, invoices, invoice_items), and loads it.

Usage:
    python scripts/load_data.py --db-url postgres://user:pass@localhost:5432/retail

Requires: pip install pandas openpyxl psycopg2-binary
"""

import argparse
import os
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


UCI_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
KAGGLE_FALLBACK = "https://www.kaggle.com/api/v1/datasets/download/vijayuv/onlineretail"


def download_dataset(dest_dir: str) -> str:
    """Download UCI Online Retail dataset. Returns path to Excel file."""
    zip_path = os.path.join(dest_dir, "online_retail.zip")
    xlsx_path = os.path.join(dest_dir, "Online Retail.xlsx")

    if os.path.exists(xlsx_path):
        print(f"  Dataset already exists at {xlsx_path}")
        return xlsx_path

    print(f"  Downloading from UCI archive...")
    try:
        urllib.request.urlretrieve(UCI_URL, zip_path)
    except Exception as e:
        print(f"  UCI download failed ({e}), trying Kaggle fallback...")
        urllib.request.urlretrieve(KAGGLE_FALLBACK, zip_path)

    print(f"  Extracting...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(dest_dir)

    if not os.path.exists(xlsx_path):
        # Kaggle may have different structure
        for f in Path(dest_dir).rglob("*.xlsx"):
            xlsx_path = str(f)
            break

    return xlsx_path


def create_schema(conn):
    """Create the 4-table schema."""
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS invoice_items CASCADE;
        DROP TABLE IF EXISTS invoices CASCADE;
        DROP TABLE IF EXISTS products CASCADE;
        DROP TABLE IF EXISTS customers CASCADE;

        CREATE TABLE customers (
            id SERIAL PRIMARY KEY,
            customer_ref TEXT NOT NULL UNIQUE,
            country TEXT NOT NULL,
            first_seen TIMESTAMPTZ NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL,
            total_orders INTEGER NOT NULL DEFAULT 0,
            total_spent NUMERIC(12,2) NOT NULL DEFAULT 0,
            is_churned BOOLEAN NOT NULL DEFAULT FALSE
        );

        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            stock_code TEXT NOT NULL UNIQUE,
            description TEXT,
            avg_price NUMERIC(10,2),
            times_sold INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE invoices (
            id SERIAL PRIMARY KEY,
            invoice_ref TEXT NOT NULL,
            customer_id INTEGER REFERENCES customers(id),
            invoice_date TIMESTAMPTZ NOT NULL,
            total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
            item_count INTEGER NOT NULL DEFAULT 0,
            is_cancellation BOOLEAN NOT NULL DEFAULT FALSE,
            country TEXT NOT NULL
        );
        CREATE INDEX idx_invoices_customer ON invoices(customer_id);
        CREATE INDEX idx_invoices_date ON invoices(invoice_date);

        CREATE TABLE invoice_items (
            id SERIAL PRIMARY KEY,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id),
            product_id INTEGER REFERENCES products(id),
            quantity INTEGER NOT NULL,
            unit_price NUMERIC(10,2) NOT NULL,
            line_total NUMERIC(12,2) NOT NULL
        );
        CREATE INDEX idx_invoice_items_invoice ON invoice_items(invoice_id);
        CREATE INDEX idx_invoice_items_product ON invoice_items(product_id);
    """)
    conn.commit()
    cur.close()


def load_data(conn, xlsx_path: str):
    """Parse Excel and load into the 4-table schema."""
    print("  Reading Excel file...")
    df = pd.read_excel(xlsx_path, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]

    # Normalize column names (UCI dataset uses these)
    col_map = {
        "InvoiceNo": "invoice_no",
        "StockCode": "stock_code",
        "Description": "description",
        "Quantity": "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice": "unit_price",
        "CustomerID": "customer_id",
        "Country": "country",
    }
    df = df.rename(columns=col_map)
    df = df.dropna(subset=["invoice_no", "stock_code", "quantity", "unit_price", "invoice_date"])

    df["invoice_no"] = df["invoice_no"].astype(str).str.strip()
    df["stock_code"] = df["stock_code"].astype(str).str.strip()
    df["description"] = df["description"].fillna("").astype(str).str.strip()
    df["customer_id"] = df["customer_id"].fillna(0).astype(int).astype(str)
    df["country"] = df["country"].fillna("Unknown").astype(str).str.strip()
    df["line_total"] = (df["quantity"] * df["unit_price"]).round(2)
    df["is_cancellation"] = df["invoice_no"].str.startswith("C")

    cur = conn.cursor()

    # --- Products ---
    print("  Loading products...")
    products = df.groupby("stock_code").agg(
        description=("description", "first"),
        avg_price=("unit_price", "mean"),
        times_sold=("quantity", lambda x: x[x > 0].sum()),
    ).reset_index()
    products["avg_price"] = products["avg_price"].round(2)
    products["times_sold"] = products["times_sold"].fillna(0).astype(int)

    execute_values(cur, """
        INSERT INTO products (stock_code, description, avg_price, times_sold)
        VALUES %s
    """, [
        (r.stock_code, r.description[:500] if r.description else None, float(r.avg_price), int(r.times_sold))
        for r in products.itertuples()
    ])
    conn.commit()

    # Build product lookup
    cur.execute("SELECT stock_code, id FROM products")
    product_map = dict(cur.fetchall())

    # --- Customers ---
    print("  Loading customers...")
    # Filter to rows with real customer IDs
    cust_df = df[df["customer_id"] != "0"].copy()
    max_date = cust_df["invoice_date"].max()

    customers = cust_df.groupby("customer_id").agg(
        country=("country", "first"),
        first_seen=("invoice_date", "min"),
        last_seen=("invoice_date", "max"),
        total_orders=("invoice_no", "nunique"),
        total_spent=("line_total", lambda x: x[~cust_df.loc[x.index, "is_cancellation"]].sum()),
    ).reset_index()
    customers["total_spent"] = customers["total_spent"].round(2)
    customers["is_churned"] = (max_date - customers["last_seen"]).dt.days > 90

    execute_values(cur, """
        INSERT INTO customers (customer_ref, country, first_seen, last_seen, total_orders, total_spent, is_churned)
        VALUES %s
    """, [
        (r.customer_id, r.country, r.first_seen, r.last_seen, int(r.total_orders), float(r.total_spent), bool(r.is_churned))
        for r in customers.itertuples()
    ])
    conn.commit()

    # Build customer lookup
    cur.execute("SELECT customer_ref, id FROM customers")
    customer_map = dict(cur.fetchall())

    # --- Invoices ---
    print("  Loading invoices...")
    invoices = df.groupby("invoice_no").agg(
        customer_id=("customer_id", "first"),
        invoice_date=("invoice_date", "first"),
        total_amount=("line_total", "sum"),
        item_count=("quantity", "count"),
        is_cancellation=("is_cancellation", "first"),
        country=("country", "first"),
    ).reset_index()
    invoices["total_amount"] = invoices["total_amount"].round(2)

    invoice_rows = []
    for r in invoices.itertuples():
        cust_id = customer_map.get(r.customer_id)
        invoice_rows.append((
            r.invoice_no, cust_id, r.invoice_date,
            float(r.total_amount), int(r.item_count),
            bool(r.is_cancellation), r.country,
        ))

    execute_values(cur, """
        INSERT INTO invoices (invoice_ref, customer_id, invoice_date, total_amount, item_count, is_cancellation, country)
        VALUES %s
    """, invoice_rows)
    conn.commit()

    # Build invoice lookup
    cur.execute("SELECT invoice_ref, id FROM invoices")
    invoice_map = dict(cur.fetchall())

    # --- Invoice Items ---
    print("  Loading invoice items (this takes a moment)...")
    item_rows = []
    for r in df.itertuples():
        inv_id = invoice_map.get(r.invoice_no)
        prod_id = product_map.get(r.stock_code)
        if inv_id:
            item_rows.append((
                inv_id, prod_id, int(r.quantity),
                float(r.unit_price), float(r.line_total),
            ))

    # Batch insert
    batch_size = 10000
    for i in range(0, len(item_rows), batch_size):
        execute_values(cur, """
            INSERT INTO invoice_items (invoice_id, product_id, quantity, unit_price, line_total)
            VALUES %s
        """, item_rows[i:i + batch_size])
        conn.commit()
        if (i // batch_size) % 10 == 0:
            print(f"    {i + batch_size}/{len(item_rows)} items...")

    cur.close()


def print_summary(conn):
    """Print table counts."""
    cur = conn.cursor()
    for table in ["customers", "products", "invoices", "invoice_items"]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count:,} rows")
    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Load UCI Online Retail into PostgreSQL")
    parser.add_argument("--db-url", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--data-dir", default=None, help="Directory to download data to (default: temp dir)")
    args = parser.parse_args()

    data_dir = args.data_dir or tempfile.mkdtemp(prefix="omx_bench_")

    print("Step 1: Download dataset")
    xlsx_path = download_dataset(data_dir)

    print("Step 2: Connect to database")
    conn = psycopg2.connect(args.db_url)

    print("Step 3: Create schema")
    create_schema(conn)

    print("Step 4: Load data")
    load_data(conn, xlsx_path)

    print("\nDone. Table counts:")
    print_summary(conn)

    conn.close()
    print(f"\nDatabase ready. Run the benchmark with:")
    print(f"  python run_bench.py --strategies omx_agent --model claude-sonnet-4-6 --iterations 3")


if __name__ == "__main__":
    main()
