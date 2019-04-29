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

## How To use AutoEDADB

* Clone or download the package.
* Create two connections as described [here](docs/Connections.md) to a source database and to the metadata database.
    * Source database: This is the DB you want to explore. You don't need any additional information, just a valid connection to the database.
    * Metadata database: It can be created if not exists. This database will store the information from the source databases.
* Edit the call of `describe_server(<YOUR_SERVER>)`.
* Run it with `python explorer.py` 

## To Do
- [x] Using samples for large tables.
- [ ] Update frequencies at once after collecting all the distinct values.
- [ ] Encapsulate SQL code and reference it by engine: 'sqlserver', 'mysql', 'postgres', 'sqlite', etc.
- [ ] Add multithreading  processing to the queries.
- [ ] Resume mode, now it deletes and insert again.