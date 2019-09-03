import aeda
from string_connections.connections import DB_META_CONFIG, DB_EMPLOYEE_CONFIG

# ['mssqlserver', 'mysql', 'postgres', 'sqlite']
SOURCE_ENGINE = 'mysql' # or one of the above
METADATA_ENGINE = 'mysql' # or one of the above

# Edit with your connections
source_connection_params = 'string_connections/connections.py'
metadata_connection_params = 'string_connections/connections.py'

aeda.setSourceConnection(SOURCE_ENGINE, DB_EMPLOYEE_CONFIG)
aeda.setMetadataConnection(METADATA_ENGINE, DB_META_CONFIG)

aeda.test_source_connection()
aeda.test_metadata_connection()

# Edit with your server name
aeda.describe_server('PyData-Meetup', 'def', 'employees')