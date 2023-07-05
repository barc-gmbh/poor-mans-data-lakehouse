# Poor Man's Data Lakehouse
A very simple demo of how query parquet files structures only the fly using DuckDB.
So it's not really a data lakehouse using Delta, Hudi or Iceberg, but it uses the same 
basic technology of making a bunch of data files accessible for querying.

Folder **data** acts as a data table and can contain multiple folders, subfolder and files (ideally all with the same structure). For simplicity and fact cloning, only 2 few larger files have been added.

Enjoy...
