"""
Legacy System Data Extractor (Mock)

This module generates a representative CSV dataset simulating a legacy Hive metastore extract.
The data includes core transaction attributes required for downstream migration and 
reconciliation processes.
"""

import csv
import random
from datetime import datetime, timedelta

def generate_legacy_extract(num_records: int = 50000, output_path: str = "legacy_trades.csv") -> None:
    """
    Generates deterministic mock historical trade records and outputs them as a CSV.
    
    Args:
        num_records (int): The number of trade records to generate. Default is 50000.
        output_path (str): The destination path for the generated CSV.
    """
    symbols = ['GS', 'AAPL', 'MSFT', 'AMZN', 'TSLA', 'JPM', 'BAC']
    exchanges = ['NYSE', 'NASDAQ', 'CBOE', 'BATS']
    
    with open(output_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        # Header Row matching the architectural requirements: trade_id, symbol, volume, price, and execution_date
        writer.writerow(['trade_id', 'symbol', 'volume', 'price', 'trade_type', 'status', 'exchange', 'execution_date'])
        
        start_date = datetime(2023, 1, 1)
        
        for i in range(num_records):
            trade_id = f"TRD-{1000000 + i}"
            symbol = random.choice(symbols)
            price = round(random.uniform(10.0, 5000.0), 2)
            volume = random.randint(10, 10000)
            trade_type = random.choice(['BUY', 'SELL'])
            status = 'SETTLED'
            exchange = random.choice(exchanges)
            execution_date = (start_date + timedelta(minutes=i*15)).strftime('%Y-%m-%d %H:%M:%S')
            
            writer.writerow([trade_id, symbol, volume, price, trade_type, status, exchange, execution_date])

    print(f"Extraction complete: {num_records} records written to {output_path}")

if __name__ == "__main__":
    generate_legacy_extract()
