import pandas as pd
import numpy as np


def do_generate(start_date: str, end_date: str, frequency: str):
  # Step 1: Create a date range
  date_range = pd.date_range(start=start_date, end=end_date, freq=frequency)

  # Step 2: Generate sample data
  np.random.seed(0)
  data = {
      'temp': np.random.randint(10, 30, size=len(date_range)),
      'humidity': np.random.randint(0, 100, size=len(date_range)),
      'var1': np.random.randn(len(date_range)),
      'var2': np.random.rand(len(date_range)),
      'var3': np.random.randint(0, 100, size=len(date_range)),
  }

  # Step 3: Create a DataFrame
  df = pd.DataFrame(data, index=date_range).reset_index().rename(columns={'index': 'timestamp'})

  # Step 4: Save the DataFrame to a CSV and Parquet files
  csv_file_path = f'out/data_{frequency}.csv'
  df.to_csv(csv_file_path, index=False)
  print(f"Time series data saved to {csv_file_path}")
  parquet_file_path = f'out/data_{frequency}.parquet'
  df.to_parquet(parquet_file_path, index=False)
  print(f"Time series data saved to {parquet_file_path}")

frequencies = ['D', 'h', 'min'] # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases

for freq in frequencies:
  do_generate('2023-01-01', '2023-12-31', freq)

