# *****************************************************************************
# BARC - Poor Man's Data Lakehouse - by Thomas Zeutschler (2022.12)
# ---
# How to query larger parquet datasets with SQL using DuckDB using PyArrow.
# Benchmark against your current approach (database, Spark etc.) and smile :-)
# *****************************************************************************
import time
import duckdb
import pyarrow.dataset as ds
import pyarrow.compute as pc

# Spin up an in-memory DuckDB instance
con = duckdb.connect()

# Link to parquet files using an Arrow Dataset (no data will be loaded yet)
my_arrow_dataset = ds.dataset('data/')

# ***************************************************************
# Use case 1 - Querying all parquet files directly with DuckDB
# ***************************************************************
print(f"1. Querying all Parquet files directly with DuckDB.")
start = time.time()
results = con.execute("SELECT count(*), avg(trip_distance) FROM my_arrow_dataset").arrow()
duration = time.time() - start
print(f"DuckDb from file-based dataset - count = {results[0][0]}, avg = {results[1][0]} in {duration:.6f} sec")


# ***************************************************************
# Use case 2 - Querying Parquet files using a filter with DuckDB
# ***************************************************************
# Define the filter to be applied while scanning
scanner_filter = (pc.field("VendorID") == pc.scalar(6))
arrow_scanner = ds.Scanner.from_dataset(my_arrow_dataset, filter=scanner_filter)
print(f"\n2. Querying Parquet files using a filter/scanner with DuckDB. Very memory efficient, recommended for very large data sets.")
# Query the Apache Arrow scanner "arrow_scanner" and return as an Arrow Table
start = time.time()
results = con.execute("SELECT count(*), avg(trip_distance) FROM arrow_scanner").arrow()
duration = time.time() - start
print(f"DuckDb with filter/scanner from file-based dataset (1st run) - count = {results[0][0]}, avg = {results[1][0]} in {duration:.6f} sec")


# ***************************************************************
# Use case 3 - Querying data in-memory with DuckDB
# ***************************************************************
print(f"\n3. Querying data in-memory with DuckDB and where clause (instead of a filter/scanner).")
# loading full data into memory
start = time.time()
arrow_table = my_arrow_dataset.to_table()
duration = time.time() - start
print(f"Arrow load dataset into in-memory arrow table in {duration:.6f} sec")

# Execute a query
start = time.time()
results = con.execute("SELECT count(*), avg(trip_distance) FROM arrow_table WHERE VendorID = 6").fetch_arrow_table()
duration = time.time() - start
print(f"DuckDb from in-memory arrow table - count = {results[0][0]}, avg = {results[1][0]} in {duration:.6f} sec")
