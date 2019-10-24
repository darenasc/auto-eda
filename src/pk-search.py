import aeda
from itertools import combinations
from tqdm import tqdm
#from tqdm import tqdm_notebook as tqdm
import pandas as pd
from sqlalchemy import create_engine
import logging

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(filename='pk-search.log', level=logging.INFO, format=FORMAT)
#logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

"""
TO DO:
- estimate combinations before processing them, to discard those options that can't
satisfy the number of rows.
- test if random sampling is more efficient than top n rows.
- save the logging in a file. (not working)
"""
SOURCE_ENGINE = 'mssqlserver' # or one of the above
METADATA_ENGINE = 'mssqlserver' # or one of the above

# Edit with your connections
source_connection_params = './string_connections/<CONNECTION_PARAMETERS_TO_SOURCE>'
metadata_connection_params = './string_connections/<CONNECTION_PARAMETERS_TO_METADATA>'

aeda.setSourceConnection(SOURCE_ENGINE, source_connection_params)
aeda.setMetadataConnection(METADATA_ENGINE, metadata_connection_params)

#aeda.test_source_connection()
#aeda.test_metadata_connection()

# The string_connections/<CONNECTION_PARAMETERS_TO_SOURCE> file has the following line (for MSSQLSERVER)
# mssql+pyodbc://<DOMAIN>\<USER_NAME>:<PASSWORD>@<DATABASE>
string_connection_file = 'string_connections/<CONNECTION_PARAMETERS_TO_SOURCE>'
with open(string_connection_file, 'r') as cs:
    connection_engine_metadata = cs.read().replace('\n', '')

source_engine = create_engine(connection_engine_metadata)
connection = source_engine.connect()

#FUNCTIONS

def run_query_on_source(query):
    """
    Executes query on source server and returns the result.
    This shold be used only for queries that returns 1 row such as counts or summaries for 
    performance reasons.
    """
    conn_source = aeda.get_db_connection(source_connection_params)
    cursor_source = aeda.get_db_cursor(conn_source)
    cursor_source.execute(query)
    rows = cursor_source.fetchall()
    cursor_source.close()
    conn_source.close()
    return rows

def get_columns_for_pk_search(server_name, table_catalog, table_schema, table_name):
    """
    Returns a list of columns from the metadata database.
    Ignores columns with NULL values and return columns with more than one unique value.
    Ignores columns with money data type.
    """
    conn_metadata = aeda.get_db_connection(metadata_connection_params)
    cursor_metadata = conn_metadata.cursor()
    sql = """select column_name 
            from uniques 
            where SERVER_NAME = '{}'
                AND TABLE_CATALOG = '{}'
                AND TABLE_SCHEMA = '{}'
                AND TABLE_NAME = '{}'
                and null_values = 0
                and DISTINCT_VALUES > 1 
                and DATA_TYPE not in ('money')
                order by ORDINAL_POSITION;""".format(server_name, table_catalog, table_schema, table_name)
    cursor_metadata.execute(sql)
    rows = cursor_metadata.fetchall()
    columns = [c[0] for c in rows]
    cursor_metadata.close()
    conn_metadata.close()
    return columns

def get_df_sql(sql, connection):
    """
    Returns a dataframe with the results of a query.
    Used to execute evaluations of the PKs on a subsample of the data.
    """
    connection = source_engine.connect()
    df = pd.read_sql_query(sql, connection)
    connection.close()
    return df

def get_sql_count(columns, top_n, table_name):
    if len(columns) == 1:
        sql = """select count(distinct {}) as n1, count(*) as n2 from (select top {} * from {}) as t;""".format(', '.join(columns), top_n, table_name)
    else:
        sql = """select count(distinct concat({})) as n1, count(*) as n2 from (select top {} * from {}) as t;""".format(', '.join(columns), top_n, table_name)
    return sql

def get_sql(fields, table_name, top_n=10_000):
    if isinstance(fields, list):
        if top_n > 0:
            sql = """select count(distinct concat({})) as n1, count(*) as n2 from (select top {} * from {}) as t;""".format(', '.join(fields), top_n, table_name)
        else:
            sql = """select count(distinct concat({})) as n1, count(*) as n2 from {};""".format(', '.join(fields), table_name)
    else:
        if top_n > 0:
            sql = """select count(distinct concat({})) as n1, count(*) as n2 from (select top {} * from {}) as t;""".format(fields, top_n, table_name)
        else:
            sql = """select count(distinct concat({})) as n1, count(*) as n2 from {};""".format(fields, table_name)
    return sql

# get a dataset and compare uniques in Python
def get_sql_sample(table_name, top_n):
    sql = """select top {} * from {};""".format(top_n, table_name)
    return sql

def get_column_combinations(columns, k = 5):
    """
    Returns all possible combinations using k columns.
    """
    results = combinations(columns, k)
    return results

def get_unique_values(columns):
    """
    Returns the number of unique values in a dataframe given a selection of columns.
    """
    records = len(df.groupby(list(columns)).size().reset_index(name='Freq'))
    return records

def count_iterable(i):
    """
    Returns the number of elements in an iterable.
    Used to get the number of combinations to test.
    """
    return sum(1 for e in i)

# Initializing settings
# server_name, table_catalog, table_schema, table_name parameters should exist in the metadata database.
# This is temporal

server_name=''
table_catalog=''
table_schema=''
table_name=''

# Search space of columns
columns = get_columns_for_pk_search(server_name, table_catalog, table_schema, table_name)
#This applies for GeneralLedger only
not_include = ['<List of columns you dont want to use in the search>']
columns = [c for c in columns if c not in not_include]
# You can add more criterias to filter the list of columns based on expert knowledge of the data source

# Creating 3 datasets for testing
logger.info('Creating a 10k dataset')
sql = get_sql_sample(table_name, 10_000)
df_10k = get_df_sql(sql, connection) # memory usage: ~27.3 MB
logger.info('10k dataset created')

logger.info('Creating a 100k dataset')
sql = get_sql_sample(table_name, 100_000)
df_100k = get_df_sql(sql, connection) # memory usage: ~244.6 MB 
logger.info('100k dataset created')

logger.info('Creating a 1M dataset')
sql = get_sql_sample(table_name, 1_000_000)
df_1M = get_df_sql(sql, connection) # memory usage: ~2.0 GB
logger.info('1M dataset created')

# Algorithm, it sends the results to the a log file
threshold = 0.99999
all_candidates = []
for number_of_columns in [1,2,3,4,5]:
    logger.info('Searching PKs in combinations of {} columns'.format(number_of_columns))
    
    candidates_10k = []
    candidates_100k = []
    candidates_1M = []
    
    for_counts = get_column_combinations(columns, number_of_columns)
    tot_combinations = count_iterable(for_counts)
    column_combinations = get_column_combinations(columns, number_of_columns)
    
    for item in tqdm(column_combinations, total=tot_combinations, unit='checks'):
        records = len(df_10k.groupby(list(item)).size().reset_index(name='Freq'))
        if records / df_10k.shape[0] >= threshold:
            candidates_10k.append(item)
    logger.info('10K dataset: {:,} candidates out of {:,} posibilities tested in {:,} records'.format(len(candidates_10k), tot_combinations, df_10k.shape[0]))
    
    if len(candidates_10k) > 0:
        logger.info('100k searching {:,} candidates on {:,} records'.format(len(candidates_10k), df_100k.shape[0]))
        candidates_100k = []
        for candidate in tqdm(candidates_10k):
            records_100k = len(df_100k.groupby(list(candidate)).size().reset_index(name='Freq'))
            if records_100k / df_100k.shape[0] > threshold:
                candidates_100k.append(candidate)
        logger.info('100K dataset: {:,} candidates out of {:,} posibilities tested in {:,} records'.format(len(candidates_100k), len(candidates_10k), df_100k.shape[0]))
                
    if len(candidates_100k) > 0:
        logger.info('1M searching {:,} candidates on {:,} records'.format(len(candidates_100k), df_1M.shape[0]))
        candidates_1M = []
        for candidate in tqdm(candidates_100k):
            records_1M = len(df_1M.groupby(list(candidate)).size().reset_index(name='Freq'))
            if records_1M / df_1M.shape[0] > threshold:
                logger.info('Candidate: {} Unique: {:,} Percentage: {:.5%}'.format(candidate, records_1M, records_1M / df_1M.shape[0]))
                candidates_1M.append(candidate)
                all_candidates.append(candidate)
        logger.info('1M dataset: {:,} candidates out of {:,} posibilities tested in {:,} records'.format(len(candidates_1M), len(candidates_100k), df_1M.shape[0]))
