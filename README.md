# Auto-EDA
Automated Exploratory Data Analysis. Simplifying Data Exploration.

You can check some examples in the [documentation](docs/Documentation.md).

Basic data exploration on databases currently supporting:
- [x] MSSQL Server
- [x] MySQL
- [x] SQLite
- [x] PostgreSQL
- [ ] Oracle

Given two connections, a source and target database, it will collect metadata for a exploration such as:
* Number of rows and columns.
* Number of distinct values and nulls per column.
* Distribution of the categorical variables.
* Statistics of the numerical variables.
* Trends from time series data.

The metadata from the `source` database will be stored in a `metadata` database that it will be accesible for any visualization tool to explore it.

## How To use AutoEda
* Clone or download the package.
* Create a connection to a source database and another one to a metadata database.
* Run `describe_database()` or `describe_table()` functions to get the metadata from the `source` using your connections.

## To Do
- [x] Using samples for large tables.
- [ ] Update frequencies at once after collecting all the distinct values.
- [ ] Encapsulate SQL code and reference it by engine: 'sqlserver', 'mysql', 'postgres', 'sqlite', etc.
- [ ] Add multithreading  processing to the queries.
- [ ] Resume mode, now it deletes and insert again.