import aeda

# ['mssqlserver', 'mysql', 'postgres', 'sqlite']
SOURCE_ENGINE = 'mssqlserver' # or one of the above
METADATA_ENGINE = 'mssqlserver' # or one of the above

source_connection_params = 'string_connections/<YOUR_SOURCE_STRING_CONNECTION>'
metadata_connection_params = 'string_connections/<YOUR_METADATA_STRING_CONNECTION>'

aeda.setSourceConnection(SOURCE_ENGINE, source_connection_params)
aeda.setMetadataConnection(METADATA_ENGINE, metadata_connection_params)

aeda.test_source_connection()
aeda.test_metadata_connection()

aeda.describe_server('<YOUR_SERVER_NAME>')