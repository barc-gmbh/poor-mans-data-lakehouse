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

# Link to parquet files using an Arrow Dataset
my_arrow_dataset = ds.dataset('data/')

# Define the filter to be applied while scanning
scanner_filter = (pc.field("VendorID") == pc.scalar(6))
arrow_scanner = ds.Scanner.from_dataset(my_arrow_dataset, filter=scanner_filter)

# ***************************************************************
# Use case 1 - Querying Parquet files directly with DuckDB
# ***************************************************************
print(f"Querying Parquet files directly with DuckDB")
# (Cold scan of files)
# Query the Apache Arrow scanner "arrow_scanner" and return as an Arrow Table
start = time.time()
results = con.execute("SELECT avg(trip_distance) FROM arrow_scanner").arrow()
duration = time.time() - start
print(f"DuckDb from file-based dataset (1st run) - avg = {results[0][0]} in {duration:.6f} sec")

# (Warm Scan of files = the OS has cached some file data)
# Query the Apache Arrow scanner "arrow_scanner" and return as an Arrow Table
start = time.time()
results = con.execute("SELECT avg(trip_distance) FROM arrow_scanner").arrow()
duration = time.time() - start
print(f"DuckDb from file-based dataset (2nd run) - avg = {results[0][0]} in {duration:.6f} sec")


# ***************************************************************
# Use case 2 - Querying data in-memory with DuckDB
# ***************************************************************
print(f"\nQuerying data in-memory with DuckDB")
# loading full data into memory
start = time.time()
arrow_table = my_arrow_dataset.to_table()
duration = time.time() - start
print(f"Arrow load dataset into in-memory arrow table in {duration:.6f} sec")

# Execute a query
start = time.time()
results = con.execute("SELECT avg(trip_distance) FROM arrow_table WHERE VendorID = 6").fetch_arrow_table()
duration = time.time() - start
print(f"DuckDb from in-memory arrow table - avg = {results[0][0]} in {duration:.6f} sec")
