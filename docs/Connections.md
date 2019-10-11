# Connections

Connections are stoder in `string_connections/` folder.

Should be of the type:

## ODBC 

If you have an ODBC created pointed to the database

    DSN=<YOUR_ODBC_NAME>

If you had it installed, you can use it directly from the code creating the following string_connection.

DRIVER={ODBC Driver 13 for SQL Server};SERVER=<FQDN_OF_THE_SERVER>;DATABASE=<YOUR_DATABASE>;UID=<YOUR_USER>;PWD=<YOUR_PASSWORD>

