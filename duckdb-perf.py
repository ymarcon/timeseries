import timeit
import duckdb


def read_csv(path: str):
  print(f"Reading CSV file from {path}")
  # Connect to DuckDB
  con = duckdb.connect(database=':memory:')  # Using an in-memory database
  # Read a CSV file into DuckDB
  con.execute(f"CREATE TABLE time_series_table AS SELECT * FROM read_csv_auto('{path}')")
  return con

def read_parquet(path: str):
  print(f"Reading Parquet file from {path}")
  # Connect to DuckDB
  con = duckdb.connect(database=':memory:')  # Using an in-memory database
  # Read a Parquet file into DuckDB
  con.execute(f"CREATE TABLE time_series_table AS SELECT * FROM '{path}'")
  return con

# Measure the file load time
print(">>> Measure the CSV/Parquet files load time")
frequencies = ['D', 'h', 'min']
for freq in frequencies:
  execution_time = timeit.timeit(f'read_csv("out/data_{freq}.csv")', number=1, globals=globals())
  print(f"Execution time: {execution_time:.3f} seconds")
  execution_time = timeit.timeit(f'read_parquet("out/data_{freq}.parquet")', number=1, globals=globals())
  print(f"Execution time: {execution_time:.3f} seconds")

print(">>> Calculate means")

# Execute the query and fetch the results
# for group_by in [{'format': '%Y-%m-%d', 'name': 'day'}, {'format': '%Y-%m', 'name': 'month'}, {'format': '%Y-%m-%d %H', 'name': 'hour'}]:
#   for col in ['temp']: #, 'humidity', 'var1', 'var2', 'var3']:
#     # Query to calculate monthly mean values
#     query = f"""
#     SELECT 
#         strftime(timestamp, '{group_by['format']}') AS {group_by['name']},
#         AVG({col}) AS {col}_mean
#     FROM time_series_table
#     GROUP BY {group_by['name']}
#     ORDER BY {group_by['name']}
#     """

def measure_exec_time(con):
  execution_time = timeit.timeit(f'con.execute(query).fetchdf()', number=1, globals=globals())
  print(f"Execution time: {execution_time:.3f} seconds")
  monthly_mean_df = con.execute(query).fetchdf()
  print(monthly_mean_df)

query = """
SELECT
  strftime(timestamp, '%Y-%m-%d %H') AS hour,
  AVG(temp) AS temp_mean
FROM time_series_table
GROUP BY hour
ORDER BY hour
"""

con = read_csv("out/data_min.csv")
measure_exec_time(con)

con = read_parquet("out/data_min.parquet")
measure_exec_time(con)