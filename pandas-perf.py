import timeit
import pandas as pd


def read_csv(path: str):
  print(f"Reading CSV file from {path}")
  return pd.read_csv(path)

def read_parquet(path: str):
  print(f"Reading Parquet file from {path}")
  return pd.read_parquet(path)

# Measure the file load time
print(">>> Measure the CSV/Parquet files load time")
frequencies = ['D', 'h', 'min']
for freq in frequencies:
  execution_time = timeit.timeit(f'read_csv("out/data_{freq}.csv")', number=1, globals=globals())
  print(f"Execution time: {execution_time:.3f} seconds")
  execution_time = timeit.timeit(f'read_parquet("out/data_{freq}.parquet")', number=1, globals=globals())
  print(f"Execution time: {execution_time:.3f} seconds")

print(">>> Calculate hourly mean")

def calculate_hourly_mean(df: pd.DataFrame):
  # Resample the data by hour and calculate the mean
  hourly_mean_df = df.resample('H').mean()
  return hourly_mean_df

def measure_exec_time(df: pd.DataFrame):
  execution_time = timeit.timeit(f'calculate_hourly_mean(df)', number=1, globals=globals())
  print(f"Execution time: {execution_time:.3f} seconds")
  hourly_mean_df = calculate_hourly_mean(df)
  # Print the results
  print(hourly_mean_df)

df = read_csv("out/data_min.csv")
# Convert the timestamp column to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)
measure_exec_time(df)

df = read_parquet("out/data_min.parquet")
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)
measure_exec_time(df)
