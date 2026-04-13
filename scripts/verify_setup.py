"""Verify benchmark setup is ready to run.

Checks:
  1. Database tables exist and have rows
  2. OnlyMetrix API is reachable
  3. Metrics are loaded
  4. One test query works end to end

Usage:
    python scripts/verify_setup.py --db-url postgres://user:pass@localhost:5432/retail
    python scripts/verify_setup.py --api-url http://localhost:3001
"""

import argparse
import sys


def check_database(db_url: str) -> bool:
    """Check database tables exist and have data."""
    try:
        import psycopg2
    except ImportError:
        print("  SKIP: psycopg2 not installed (pip install psycopg2-binary)")
        return True

    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        expected = {
            "customers": 4000,
            "products": 3000,
            "invoices": 20000,
            "invoice_items": 400000,
        }

        all_ok = True
        for table, min_rows in expected.items():
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            ok = count >= min_rows
            status = "OK" if ok else "FAIL"
            print(f"  {status}: {table} has {count:,} rows (expected >= {min_rows:,})")
            if not ok:
                all_ok = False

        cur.close()
        conn.close()
        return all_ok

    except Exception as e:
        print(f"  FAIL: Could not connect to database: {e}")
        return False


def check_api(api_url: str) -> bool:
    """Check OnlyMetrix API is running and metrics are loaded."""
    try:
        import httpx
    except ImportError:
        print("  SKIP: httpx not installed (pip install httpx)")
        return True

    # Check metrics endpoint
    try:
        resp = httpx.get(f"{api_url}/v1/metrics", timeout=5)
        if resp.status_code != 200:
            print(f"  FAIL: /v1/metrics returned {resp.status_code}")
            return False

        metrics = resp.json().get("metrics", [])
        print(f"  OK: {len(metrics)} metrics loaded")

        if len(metrics) < 15:
            print(f"  WARN: Expected at least 15 metrics, got {len(metrics)}")

    except Exception as e:
        print(f"  FAIL: Could not reach {api_url}/v1/metrics: {e}")
        return False

    # Test one query
    try:
        resp = httpx.post(
            f"{api_url}/v1/metrics/total_revenue",
            json={},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"  FAIL: total_revenue query returned {resp.status_code}")
            return False

        rows = resp.json().get("rows", [])
        if not rows:
            print("  FAIL: total_revenue returned no rows")
            return False

        value = list(rows[0].values())[0]
        print(f"  OK: total_revenue = {value}")
        return True

    except Exception as e:
        print(f"  FAIL: Could not query total_revenue: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Verify benchmark setup")
    parser.add_argument("--db-url", default=None, help="PostgreSQL connection URL")
    parser.add_argument("--api-url", default="http://localhost:3001", help="OnlyMetrix API URL")
    args = parser.parse_args()

    all_ok = True

    if args.db_url:
        print("Checking database...")
        if not check_database(args.db_url):
            all_ok = False
        print()

    print("Checking OnlyMetrix API...")
    if not check_api(args.api_url):
        all_ok = False
    print()

    if all_ok:
        print("Setup verified. Ready to run benchmark.")
        print(f"  python run_bench.py --strategies sql omx_agent --model claude-sonnet-4-6 --iterations 3")
    else:
        print("Setup failed. Fix the issues above and re-run.")
        sys.exit(1)


if __name__ == "__main__":
    main()
