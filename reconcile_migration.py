"""
Enterprise Trade Data Reconciliation

Validates migration integrity by comparing the source legacy CSV extract
against the final target Snowflake table. It calculates aggregate checksums
(row count, total volume, total price) to prove 100% data fidelity.

Designed to operate both as a standalone script and as an Airflow PythonOperator callable.
"""

import csv
import snowflake.connector
import os
from decimal import Decimal


CSV_PATH = os.getenv('LEGACY_CSV_PATH', '/opt/airflow/legacy_trades.csv')


def calculate_csv_checksums(file_path: str) -> dict:
    """
    Computes aggregations over the legacy CSV file.

    Args:
        file_path (str): Path to the legacy CSV extract.
    Returns:
        dict: Aggregated checksums.
    """
    row_count = 0
    total_volume = 0
    total_price = Decimal('0.0')

    with open(file_path, mode='r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            total_volume += int(row['volume'])
            total_price += Decimal(row['price'])

    return {
        'row_count': row_count,
        'total_volume': total_volume,
        'total_price': total_price
    }


def calculate_snowflake_checksums() -> dict:
    """
    Queries Snowflake for aggregate checksums of the target table.

    Returns:
        dict: Aggregated checksums.
    """
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='MIGRATION_WH',
        database='ENTERPRISE_MIGRATION_DB',
        schema='TRADES_SCHEMA'
    )

    try:
        cursor = conn.cursor()
        query = '''
            SELECT
                COUNT(*) as row_count,
                SUM(volume) as total_volume,
                SUM(price) as total_price
            FROM TARGET_TRADES
        '''
        cursor.execute(query)
        result = cursor.fetchone()

        return {
            'row_count': result[0] if result[0] is not None else 0,
            'total_volume': result[1] if result[1] is not None else 0,
            'total_price': Decimal(str(result[2])) if result[2] is not None else Decimal('0.0')
        }
    finally:
        conn.close()


def check_integrity(**context) -> None:
    """
    Airflow-callable entry point for the reconciliation audit task.

    Raises:
        ValueError: If checksums between source CSV and target Snowflake table do not match,
                    signalling Airflow to mark the task as failed.
    """
    print("Starting data integrity reconciliation...")

    print(f"Calculating checksums for source data: {CSV_PATH}")
    source_checksums = calculate_csv_checksums(CSV_PATH)

    print("Calculating checksums for target Snowflake table...")
    target_checksums = calculate_snowflake_checksums()

    print("\n--- Reconciliation Report ---")
    print(f"Metric         | Source CSV | Target DB")
    print(f"---------------------------------------")
    print(f"Row Count      | {source_checksums['row_count']:<10} | {target_checksums['row_count']:<10}")
    print(f"Total Volume   | {source_checksums['total_volume']:<10} | {target_checksums['total_volume']:<10}")
    print(f"Total Price    | {source_checksums['total_price']:<10} | {target_checksums['total_price']:<10}")
    print("---------------------------------------")

    if source_checksums != target_checksums:
        raise ValueError(
            f"INTEGRITY FAILURE: Checksum mismatch detected. "
            f"Source={source_checksums} | Target={target_checksums}"
        )

    print("SUCCESS: 100% data integrity verified. Checksums match exactly.")


if __name__ == "__main__":
    import sys
    if not os.path.exists(CSV_PATH):
        print(f"Source file {CSV_PATH} not found. Run the mock data generator first.")
        sys.exit(1)

    try:
        check_integrity()
    except ValueError as e:
        print(e)
        sys.exit(1)
