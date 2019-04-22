# Auto-EDA
Automated Exploratory Data Analysis. Simplifying Data Exploration.

You can check some examples in the [documentation](docs/Documentation.md).

Basic data exploration on databases. Currently supports:
* MSSQL Server,
* MySQL, 
* SQLite, and
* PostgreSQL.

Given a connection to a database it will collect the necessary metadata for a exploration such as:
* Number of records and columns.
* Number of distinct values and nulls.
* Distribution of the categorical variables.
* Statistics of the numerical variables.
* Trends from time series data.

The metadata from the `source` database will be stored in a `metadata` database that it will be accesible for any visualization tool to explore it.

## How To use AutoEda
* Clone or download the package.
* Create a connection to a source database and another one to a metadata database.
* Run `describe_database()` or `describe_table()` functions to get the metadata from the `source` using your connections.

## To Do
- [ ] Using samples for large tables.
- [ ] Update frequencies at once after collecting all the distinct values.
- [ ] Encapsulate SQL code and reference it by engine: 'sqlserver', 'mysql', 'postgres', 'sqlite', etc.
- [ ] Add multithreading  processing to the queries.
- [ ] Resume mode, now it deletes and insert again.