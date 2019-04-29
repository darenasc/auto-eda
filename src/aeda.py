from __future__ import division
import pyodbc
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import sqlite3
from tqdm import tqdm
import time
from termcolor import colored

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

SOURCE_ENGINE = ''
METADATA_ENGINE = ''
source_connection_params = ''
metadata_connection_params = ''

def close_db_sqlite(db):
    db.close()
    return

def get_db_sqlite(path, db_name):
    db = sqlite3.connect(path + '/' + db_name)
    return db

def create_metadata_db(path, db_name):
    """
    Creates the necessary tables to store metadata about the databases.
    """
    
    columns = '''CREATE TABLE IF NOT EXISTS columns (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , ORDINAL_POSITION INTEGER
            , DATA_TYPE TEXT)'''

    tables = '''CREATE TABLE IF NOT EXISTS tables (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , N_COLUMNS INTEGER
            , N_ROWS INTEGER)'''

    uniques = '''CREATE TABLE IF NOT EXISTS uniques (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , ORDINAL_POSITION INTEGER
            , DATA_TYPE TEXT
            , DISTINCT_VALUES INTEGER
            , NULL_VALUES INTEGER)'''

    data_values = '''CREATE TABLE IF NOT EXISTS data_values (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , DATA_VALUE TEXT
            , FREQUENCY_NUMBER INTEGER
            , FREQUENCY_PERCENTAGE FLOAT)'''

    dates = '''CREATE TABLE IF NOT EXISTS dates (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , DATA_VALUE TEXT
            , FREQUENCY_NUMBER INTEGER
            , FREQUENCY_PERCENTAGE FLOAT)'''
    
    stats = '''CREATE TABLE IF NOT EXISTS stats (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , AVG FLOAT
            , STDEV FLOAT
            , VAR FLOAT
            , SUM FLOAT
            , MAX FLOAT
            , MIN FLOAT
            , RANGE FLOAT
            , P01 FLOAT
            , P025 FLOAT
            , P05 FLOAT
            , P10 FLOAT
            , Q1 FLOAT
            , Q2 FLOAT
            , Q3 FLOAT
            , P90 FLOAT
            , P95 FLOAT
            , P975 FLOAT
            , P99 FLOAT
            , IQR FLOAT)'''

    db = get_db_sqlite(path, db_name)
    cursor = db.cursor()
    
    cursor.execute(columns)
    cursor.execute(tables)
    cursor.execute(uniques)
    cursor.execute(data_values)
    cursor.execute(dates)
    cursor.execute(stats)
    
    db.commit()
    
    cursor.close()
    db.close()
    return

# Functions to connect to databases

def get_db_connection(string_connection, verbose = False):
    with open(string_connection, 'r') as cs:
        connection_string = cs.read().replace('\n', '')

    connection = pyodbc.connect(connection_string)
    if verbose:
        logger.info('Connection established to {}'.format(string_connection.split('/')[-1]))
        logger.info('Connection string: {}'.format(connection_string))
    return connection

def get_db_cursor(connection):
    return connection.cursor()

def close_db_connection(connection):
    connection.close()
    return

def close_db_cursor(cursor):
    cursor.close()
    return

SOURCE_ENGINE = ''
METADATA_ENGINE = ''
source_connection_params = ''
metadata_connection_params = ''

def test_source_connection():
    try:
        conn_source = get_db_connection(source_connection_params)
        print('[', colored('OK', 'green'), ']', '\tConnection to the source tested successfully...')
        cursor_source = get_db_cursor(conn_source)
        print('[', colored('OK', 'green'), ']', '\tCursor to the source tested successfully...')
        return
    except:
        print('[', colored('Error', 'red'), ']', "\tCan't establish connection to the source database...")
    finally:
        cursor_source.close()
        conn_source.close()

def test_metadata_connection():
    try:
        conn_metadata = get_db_connection(metadata_connection_params)
        print('[', colored('OK', 'green'), ']', '\tConnection to the metadata database tested successfully...')
        cursor_metadata = conn_metadata.cursor()
        print('[', colored('OK', 'green'), ']', '\tCursor to the metadata database tested successfully...')
    except:
        print('[', colored('Error', 'red'), ']', "\tCan't establish connection to the metadata database...")
    finally:
        cursor_metadata.close()
        conn_metadata.close()

def setSourceConnection(engine_type, connection_string):
    global SOURCE_ENGINE
    global source_connection_params

    SOURCE_ENGINE = engine_type
    source_connection_params = connection_string
    return

def setMetadataConnection(engine_type, connection_string):
    global METADATA_ENGINE
    global metadata_connection_params

    METADATA_ENGINE = engine_type
    metadata_connection_params = connection_string
    return

def getColumnsFromServer(server_name):
    conn_metadata = get_db_connection(metadata_connection_params)
    cursor_metadata = conn_metadata.cursor()
    
    sql = """select distinct SERVER_NAME 
                , TABLE_CATALOG 
                , TABLE_SCHEMA 
                , TABLE_NAME
                from columns
                where SERVER_NAME = ?;"""
    cursor_metadata.execute(sql, (server_name,))
    rows = cursor_metadata.fetchall()
    
    cursor_metadata.close()
    conn_metadata.close()
    return rows

def insertOrUpdateColumns(conn_metadata, cursor_metadata, server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, verbose= False):
    def checkIfTableExistInColumns(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """select * from columns
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?
             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name))
        return len(cursor_metadata.fetchall())
    
    if checkIfTableExistInColumns(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """delete from columns
                WHERE SERVER_NAME = ?
                 AND TABLE_CATALOG = ?
                 AND TABLE_SCHEMA = ?
                 AND TABLE_NAME = ?
                 AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name))
        conn_metadata.commit()
    
    sql = """insert into columns (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)
             values (?, ?, ?, ?, ?, ?, ?)"""
    cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type))
    conn_metadata.commit()
    if verbose:
        logger.info('{}.{}.{}.{}.{} has been updated into columns...'.format(server_name, table_catalog, table_schema, table_name, column_name))
    return

def insertOrUpdateTables(server_name, table_catalog, table_schema, table_name, verbose = False, ignore_views = True):
    """
    Stores the number of columns and the number of rows of the table.
    Each row is one table.
    """
    conn_source = get_db_connection(source_connection_params)
    cursor_source = get_db_cursor(conn_source)

    conn_metadata = get_db_connection(metadata_connection_params)
    cursor_metadata = conn_metadata.cursor()

    def checkIfTableExistInTables(server_name, table_catalog, table_schema, table_name):
        sql = """select * from tables
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name))
        return len(cursor_metadata.fetchall())
    
    def updateNumberOfRows(server_name, table_catalog, table_schema, table_name):
        query = """select count(*) as n from {}.{}.{}""".format(table_catalog, table_schema, table_name)
        cursor_source.execute(query)
        num_rows = cursor_source.fetchone()

        sql_update = """UPDATE tables 
                        SET N_ROWS = ? 
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql_update, (num_rows[0], server_name, table_catalog, table_schema, table_name))
        conn_metadata.commit()
        return 
    
    def updateNumberOfColumns(server_name, table_catalog, table_schema, table_name):
        sql = """SELECT ? AS SERVER_NAME
        , TABLE_CATALOG
        , TABLE_SCHEMA
        , TABLE_NAME
        , COUNT(*) AS N_COLUMNS
        , CAST(NULL as INTEGER) AS N_ROWS
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = ?
         AND TABLE_SCHEMA = ?
         AND TABLE_NAME = ?
        GROUP BY TABLE_CATALOG
            , TABLE_SCHEMA
            , TABLE_NAME
        ORDER BY 1,2,3,4;"""
        cursor_source.execute(sql, (server_name, table_catalog, table_schema, table_name))
        rows = cursor_source.fetchall()
        sql_insert = """insert into tables (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, N_COLUMNS, N_ROWS)
                        values (?, ?, ?, ?, ?, ?);"""
        for row in rows:
            cursor_metadata.execute(sql_insert, (row[0], row[1], row[2], row[3], row[4], row[5]))
            conn_metadata.commit()
        return
    
    if checkIfTableExistInTables:
        sql = """delete from tables
                WHERE SERVER_NAME = ?
                 AND TABLE_CATALOG = ?
                 AND TABLE_SCHEMA = ?
                 AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name))
        conn_metadata.commit()
        
    updateNumberOfColumns(server_name, table_catalog, table_schema, table_name)
    updateNumberOfRows(server_name, table_catalog, table_schema, table_name)
        
    if verbose:
        logger.info('{}.{}.{}.{} updated into tables...'.format(server_name, table_catalog, table_schema, table_name))
    
    cursor_source.close()
    conn_source.close()

    cursor_metadata.close()
    conn_metadata.close()
    return

def insertOrUpdateUniques(server_name, table_catalog, table_schema, table_name, verbose = False):
    def checkIfTableExistInUniques(server_name, table_catalog, table_schema, table_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql = """select * from uniques
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name))
        len_uniques = len(cursor_metadata.fetchall())

        cursor_metadata.close()
        conn_metadata.close()
        return len_uniques
    
    def getColumnsFromTable(server_name, table_catalog, table_schema, table_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql_fields = """select column_name
                            , ORDINAL_POSITION
                            , DATA_TYPE 
                        from columns
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql_fields, (server_name, table_catalog, table_schema, table_name))
        rows = cursor_metadata.fetchall()

        cursor_metadata.close()
        conn_metadata.close()
        return rows
    
    def getValuesFromColumn(server_name, table_catalog, table_schema, table_name, column_name):
        conn_source = get_db_connection(source_connection_params)
        cursor_source = get_db_cursor(conn_source)

        sql_values = """select count(distinct "{}") as distinctValues
                                , sum(case when "{}" is null then 1 else 0 end) as nullValues
                        FROM    {}.{}""".format(column_name, column_name, table_schema, table_name)
        cursor_source.execute(sql_values)
        rows = cursor_source.fetchall()

        cursor_source.close()
        conn_source.close()

        return rows
    
    def insertValuesInUniques(server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, distinctValues, nullValues):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql_insert = """insert into uniques (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, DISTINCT_VALUES, NULL_VALUES)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, distinctValues, nullValues))
        conn_metadata.commit()

        cursor_metadata.close()
        conn_metadata.close()
        return
    
    def deleteExistingRows(server_name, table_catalog, table_schema, table_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql = """delete from uniques
                WHERE SERVER_NAME = ?
                 AND TABLE_CATALOG = ?
                 AND TABLE_SCHEMA = ?
                 AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name))
        conn_metadata.commit()

        cursor_metadata.close()
        conn_metadata.close()
        return
    
    if checkIfTableExistInUniques(server_name, table_catalog, table_schema, table_name):
        deleteExistingRows(server_name, table_catalog, table_schema, table_name)
        
    columns = getColumnsFromTable(server_name, table_catalog, table_schema, table_name)
    
    pbar = tqdm(columns)
    for field in pbar:
        pbar.set_description('Column {}'.format(field[0]))
        if field[2] not in ('text', 'image', 'ntext', 'blob', 'varbinary'):
            values = getValuesFromColumn(server_name, table_catalog, table_schema, table_name, field[0])
            try:
                insertValuesInUniques(server_name, table_catalog, table_schema, table_name, field[0], field[1], field[2], values[0][0], values[0][1])
            except:
                print('Problems with: {}.{}'.format(table_name, field[0]))
                pass

        if verbose:
            logger.info('{}.{}.{}.{}.{} updated into summary_v3...'.format(server_name, table_catalog, table_schema, table_name, field[0]))
    
    return

def insertOrUpdateDataValues(server_name, table_catalog, table_schema, table_name, verbose = False, threshold = 5000, with_data_sample = False, n_samples = 10000):
    """
    Stores each distinct data value of each column based on a threshould of distinct values
    (5000 distinct values by default) and has the frequency of the data value. 
    It doesn't store data of `date` types columns.
    
    SERVER_NAME 
    TABLE_CATALOG 
    TABLE_SCHEMA 
    TABLE_NAME 
    COLUMN_NAME 
    DATA_VALUE 
    FREQUENCY_NUMBER 
    FREQUENCY_PERCENTAGE
    """
    def checkIfTableExistInDataValues(server_name, table_catalog, table_schema, table_name, column_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql = """select * from data_values
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?
             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name))
        num_rows = len(cursor_metadata.fetchall())

        cursor_metadata.close()
        conn_metadata.close()
        return num_rows
    
    def getColumnsFromTable(server_name, table_catalog, table_schema, table_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql_fields = """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                        from columns
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND DATA_TYPE NOT IN ('text', 'image', 'ntext', 'blob', 'varbinary');"""
        cursor_metadata.execute(sql_fields, (server_name, table_catalog, table_schema, table_name))
        rows = cursor_metadata.fetchall()

        cursor_metadata.close()
        conn_metadata.close()
        return rows
    
    def insertFrequencyValue(server_name, table_catalog, table_schema, table_name, column_name, threshold, number_of_rows, with_data_sample = False, n_samples = 10000):
        conn_source = get_db_connection(source_connection_params)
        cursor_source = get_db_cursor(conn_source)

        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()
        
        num_distinct_values = getNumDistinctValues(server_name, table_catalog, table_schema, table_name, column_name)
        
        if num_distinct_values < threshold:
            if SOURCE_ENGINE == 'mssqlserver' and with_data_sample and number_of_rows > n_samples:
                sql_frequency = """WITH t as (
                                        select * FROM {1}.{2}.{3} TABLESAMPLE ({4} ROWS) REPEATABLE ({5})
                                    )
                                SELECT t.[{0}]
                                    , COUNT(*) AS N 
                                FROM t
                                GROUP BY t.[{0}] 
                                ORDER BY N DESC;""".format(column_name, table_catalog, table_schema, table_name, n_samples, 42)
            else:
                sql_frequency = """SELECT [{0}]
                                    , COUNT(*) AS N 
                                FROM {1}.{2}.{3} 
                                GROUP BY [{0}] 
                                ORDER BY N DESC;""".format(column_name, table_catalog, table_schema, table_name)

            cursor_source.execute(sql_frequency)
            rows = cursor_source.fetchall()

            pbar = tqdm(rows)
            for row in pbar:
                pbar.set_description('Distinct values')
                sql_insert = """insert into data_values (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                                values (?, ?, ?, ?, ?, ?, ?);"""
                try:
                    if isinstance(row[0], str):
                        cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, row[0], row[1]))
                    else:
                        cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, str(row[0]), row[1]))
                    conn_metadata.commit()
                except:
                    #print('\nProblems with {}.{}'.format(table_name, column_name))
                    pass
        cursor_source.close()
        conn_source.close()

        cursor_metadata.close()
        conn_metadata.close()
        return
    
    def updateFrequencyValue(server_name, table_catalog, table_schema, table_name, column_name, threshold, number_of_rows, with_data_sample = False, n_samples = 10000):
        conn_source = get_db_connection(source_connection_params)
        cursor_source = get_db_cursor(conn_source)

        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()
        
        num_distinct_values = getNumDistinctValues(server_name, table_catalog, table_schema, table_name, column_name)
        
        if num_distinct_values < threshold:
            if SOURCE_ENGINE == 'mssqlserver' and with_data_sample and number_of_rows > n_samples:
                sql_frequency = """WITH t as (
                                        select * FROM {1}.{2}.{3} TABLESAMPLE ({4} ROWS) REPEATABLE ({5})
                                    )
                                SELECT t.[{0}]
                                    , COUNT(*) AS N 
                                FROM t
                                GROUP BY t.[{0}] 
                                ORDER BY N DESC;""".format(column_name, table_catalog, table_schema, table_name, n_samples, 42)
            else:
                sql_frequency = """SELECT [{0}]
                                    , COUNT(*) AS N 
                                FROM {1}.{2}.{3} 
                                GROUP BY [{0}] 
                                ORDER BY N DESC;""".format(column_name, table_catalog, table_schema, table_name)

            cursor_source.execute(sql_frequency)
            rows = cursor_source.fetchall()

            pbar = tqdm(rows)
            for row in pbar:
                pbar.set_description('Distinct values')
                sql_update = """update data_values
                                set FREQUENCY_NUMBER = {6}
                                where SERVER_NAME = '{0}'
                                    and TABLE_CATALOG = '{1}'
                                    and TABLE_SCHEMA = '{2}'
                                    and TABLE_NAME = '{3}'
                                    and COLUMN_NAME = '{4}'
                                    and DATA_VALUE = '{5}';""".format(server_name, table_catalog, table_schema, table_name, column_name, row[0], row[1])
                try:
                    if isinstance(row[0], str):
                        cursor_metadata.execute(sql_update)
                    else:
                        cursor_metadata.execute(sql_update)
                    conn_metadata.commit()
                except:
                    #print('\nProblems with {}.{}'.format(table_name, column_name))
                    pass
        cursor_source.close()
        conn_source.close()

        cursor_metadata.close()
        conn_metadata.close()
        return
    
    def insertFrequencyPercentage(server_name, table_catalog, table_schema, table_name, column_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql_total = """SELECT SUM(FREQUENCY_NUMBER) AS TOTAL
                        FROM data_values
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_total, (server_name, table_catalog, table_schema, table_name, column_name))
        total = cursor_metadata.fetchall()[0][0]
        
        sql_frequency = """SELECT DATA_VALUE, FREQUENCY_NUMBER
                            FROM data_values
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_frequency, (server_name, table_catalog, table_schema, table_name, column_name))
        rows = cursor_metadata.fetchall()
        pbar = tqdm(rows)
        for row in pbar:
            pbar.set_description('Percentage')
            sql_update = """UPDATE data_values SET FREQUENCY_PERCENTAGE = ?
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?
                             AND DATA_VALUE = ?;"""
            cursor_metadata.execute(sql_update, ((row[1] / total), server_name, table_catalog, table_schema, table_name, column_name, row[0]))
            conn_metadata.commit()
        
        cursor_metadata.close()
        conn_metadata.close()    
        return
    
    def getNumDistinctValues(server_name, table_catalog, table_schema, table_name, column_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql_check_threshold = """select DISTINCT_VALUES from uniques 
                                 where SERVER_NAME = ?
                                     AND TABLE_CATALOG = ?
                                     AND TABLE_SCHEMA = ?
                                     AND TABLE_NAME = ?
                                     AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_check_threshold, (server_name, table_catalog, table_schema, table_name, column_name))
        rows = cursor_metadata.fetchone()

        cursor_metadata.close()
        conn_metadata.close()
        return rows[0]
        
    def getNumberOfRows(server_name, table_catalog, table_schema, table_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql = """select N_ROWS from tables
                WHERE SERVER_NAME = ?
                    AND TABLE_CATALOG = ?
                    AND TABLE_SCHEMA = ?
                    AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name))
        num_rows = cursor_metadata.fetchall()[0][0]

        cursor_metadata.close()
        conn_metadata.close()
        return num_rows

    def deleteExistingRows(server_name, table_catalog, table_schema, table_name, column_name):
        conn_metadata = get_db_connection(metadata_connection_params)
        cursor_metadata = conn_metadata.cursor()

        sql_delete = """delete from data_values
                    WHERE SERVER_NAME = ?
                     AND TABLE_CATALOG = ?
                     AND TABLE_SCHEMA = ?
                     AND TABLE_NAME = ?
                     AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_delete, (server_name, table_catalog, table_schema, table_name, column_name))
        conn_metadata.commit()

        cursor_metadata.close()
        conn_metadata.close()
        return
    
    columns = getColumnsFromTable(server_name, table_catalog, table_schema, table_name)
    number_of_rows = getNumberOfRows(server_name, table_catalog, table_schema, table_name)
    pbar = tqdm(columns)
    for column in pbar:
        pbar.set_description('Column %s' % column[4])
        if checkIfTableExistInDataValues(server_name, table_catalog, table_schema, table_name, column[4]) > 0:
            deleteExistingRows(server_name, table_catalog, table_schema, table_name, column[4])
        
        insertFrequencyValue(server_name, table_catalog, table_schema, table_name, column[4], threshold, number_of_rows, with_data_sample, n_samples)
        #updateFrequencyValue(server_name, table_catalog, table_schema, table_name, column[4], threshold, number_of_rows, with_data_sample, n_samples)
        #insertFrequencyPercentage(server_name, table_catalog, table_schema, table_name, column[4])
        
        if verbose:
            logger.info('{}.{}.{}.{}.{} updated into data_values...'.format(server_name, table_catalog, table_schema, table_name, column[4]))
    
    return

def insertOrUpdateDates(server_name, table_catalog, table_schema, table_name, verbose = False, thresold = 5000):
    """
    Stores each distinct data value of each column based on a threshould of distinct values
    (5000 distinct values by default) and has the frequency of the data value. 
    It stores only data of `date` or `time` types columns.
    It simplifies to group and visualise the time series data.
    
    SERVER_NAME 
    TABLE_CATALOG 
    TABLE_SCHEMA 
    TABLE_NAME 
    COLUMN_NAME 
    DATA_VALUE 
    FREQUENCY_NUMBER 
    FREQUENCY_PERCENTAGE
    """
    conn_metadata = get_db_connection(metadata_connection_params)
    cursor_metadata = conn_metadata.cursor()

    conn_source = get_db_connection(source_connection_params)
    cursor_source = get_db_cursor(conn_source)

    def checkIfTableExistInDates(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """select * from dates
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?
             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name))
        return len(cursor_metadata.fetchall())
    
    def getDatetimeColumns(server_name, table_catalog, table_schema, table_name):
        sql_datetimes = """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                            from columns 
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND DATA_TYPE IN ('datetime', 'timestamp', 'date', 'datetime2', 'smalldatetime');"""
        cursor_metadata.execute(sql_datetimes, (server_name, table_catalog, table_schema, table_name))
        return cursor_metadata.fetchall()
    
    def insertDateFrequency(server_name, table_catalog, table_schema, table_name, column_name, thresold):
        """
        This is working for MS SQL Server. 
        For other SQL engines this function should be implemented with their own date functions.
        """
        sql_agg_month = """SELECT DATEFROMPARTS(YEAR({}), MONTH({}), 1) as date, count(*) as N 
                            FROM {}.{}.{}
                            GROUP BY DATEFROMPARTS(YEAR({}), MONTH({}), 1)
                            ORDER BY N DESC;""".format(column_name, column_name, table_catalog, table_schema, table_name, column_name, column_name)
        try:
            cursor_source.execute(sql_agg_month)
            rows = cursor_source.fetchall()
            if len(rows) < thresold:
                for row in rows:
                    sql_insert = """INSERT INTO dates (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)"""
                    cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, row[0], row[1]))
                    conn_metadata.commit()
        except:
            print('Problems with: {}.{}'.format(table_name, column_name))
            pass

        return
    
    def updateFrequencyPercentage(server_name, table_catalog, table_schema, table_name, column_name):
        sql_total = """SELECT SUM(FREQUENCY_NUMBER) AS TOTAL
                        FROM dates
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_total, (server_name, table_catalog, table_schema, table_name, column_name))
        total = cursor_metadata.fetchall()[0][0]
        
        sql_frequency = """SELECT DATA_VALUE, FREQUENCY_NUMBER
                            FROM dates
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_frequency, (server_name, table_catalog, table_schema, table_name, column_name))
        rows = cursor_metadata.fetchall()
        pbar = tqdm(rows)
        for row in pbar:
            pbar.set_description('Updating {}'.format(column_name))
            sql_update = """UPDATE dates SET FREQUENCY_PERCENTAGE = ?
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?
                             AND DATA_VALUE = ?;"""
            cursor_metadata.execute(sql_update, ((row[1] / total), server_name, table_catalog, table_schema, table_name, column_name, row[0]))
            conn_metadata.commit()
        return
    
    columns = getDatetimeColumns(server_name, table_catalog, table_schema, table_name)
    pbar = tqdm(columns)
    for column in pbar:
        pbar.set_description('Column {}'.format(column[4]))
        if checkIfTableExistInDates(server_name, table_catalog, table_schema, table_name, column[4]) > 0:
            sql_delete = """delete from dates
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?;"""
            cursor_metadata.execute(sql_delete, (server_name, table_catalog, table_schema, table_name, column[4]))
            conn_metadata.commit()
        
        insertDateFrequency(server_name, table_catalog, table_schema, table_name, column[4], thresold)
        #updateFrequencyPercentage(server_name, table_catalog, table_schema, table_name, column[4])
    
        if verbose:
            logger.info('{}.{}.{}.{}.{} updated into dates...'.format(server_name, table_catalog, table_schema, table_name, column[4]))
    
    cursor_source.close()
    conn_source.close()

    cursor_metadata.close()
    conn_metadata.close()
    return

def insertOrUpdateStats(server_name, table_catalog, table_schema, table_name, verbose = False, level = 'one', with_data_sample = False, n_samples = 10000):
    """
    Three levels:
    - one: only stats
    - two: level one plus percentiles
    - three: (not implemented yet) kurtosis and skewness
    
    SERVER_NAME , TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
    , AVG 
    , STDEV
    , VAR 
    , SUM 
    , MAX 
    , MIN 
    , RANGE
    
    , P01 
    , P025
    , P05
    , P10
    , Q1 
    , Q2 
    , Q3 
    , P90 
    , P95 
    , P975
    , P99 
    , IQR 
    """
    conn_source = get_db_connection(source_connection_params)
    cursor_source = get_db_cursor(conn_source)

    conn_metadata = get_db_connection(metadata_connection_params)
    cursor_metadata = conn_metadata.cursor()

    def checkIfTableExistInStats(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """select * from stats
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?
             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name))
        return len(cursor_metadata.fetchall())
    
    def getNumericColumnsFromTable(server_name, table_catalog, table_schema, table_name):
        sql_fields = """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                        from columns 
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND DATA_TYPE IN ('int', 'decimal', 'numeric', 'float', 'money', 'tinyint', 'bigint', 'smallint', 'real');"""
        cursor_metadata.execute(sql_fields, (server_name, table_catalog, table_schema, table_name))
        return cursor_metadata.fetchall()
    
    def insertBasicStats(server_name, table_catalog, table_schema, table_name, column_name, with_data_sample = False):
        if SOURCE_ENGINE == 'mssqlserver' and with_data_sample:
            sql_stats = """with t as ( SELECT * FROM {1}.{2}.{3} TABLESAMPLE ({4} ROWS) REPEATABLE ({5})
                            )
                            SELECT  AVG(CAST("{0}" as FLOAT)) AS AVG_
                                , STDEV(CAST("{0}" as FLOAT)) as STDEV_
                                , VAR(CAST("{0}" as FLOAT)) as VAR_
                                , SUM(CAST("{0}" as FLOAT)) as SUM_
                                , MAX(CAST("{0}" as FLOAT)) AS MAX_
                                , MIN(CAST("{0}" as FLOAT)) AS MIN_
                                , MAX(CAST("{0}" as FLOAT)) - MIN(CAST("{0}" as FLOAT)) as RANGE_
                        FROM    t;""".format(column_name, table_catalog, table_schema, table_name, n_samples, 42)
        else:
            sql_stats = """SELECT  AVG(CAST("{0}" as FLOAT)) AS AVG_
                                , STDEV(CAST("{0}" as FLOAT)) as STDEV_
                                , VAR(CAST("{0}" as FLOAT)) as VAR_
                                , SUM(CAST("{0}" as FLOAT)) as SUM_
                                , MAX(CAST("{0}" as FLOAT)) AS MAX_
                                , MIN(CAST("{0}" as FLOAT)) AS MIN_
                                , MAX(CAST("{0}" as FLOAT)) - MIN(CAST("{0}" as FLOAT)) as RANGE_
                        FROM    {1}.{2}.{3};""".format(column_name, table_catalog, table_schema, table_name)
        cursor_source.execute(sql_stats)
        rows = cursor_source.fetchall()
        for row in rows:
            sql_insert = """insert into stats (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, AVG, STDEV, VAR, SUM, MAX, MIN, RANGE_)
                            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            conn_metadata.commit()
        return
    
    def updatePercentiles(server_name, table_catalog, table_schema, table_name, column_name):
        sql_percentiles = """select distinct 
                                    percentile_cont(0.01) within group (order by "{0}") over (partition by null) as P01
                                    , percentile_cont(0.025) within group (order by "{0}") over (partition by null) as P025
                                    , percentile_cont(0.05) within group (order by "{0}") over (partition by null) as P05
                                    , percentile_cont(0.10) within group (order by "{0}") over (partition by null) as P10
                                    , percentile_cont(0.25) within group (order by "{0}") over (partition by null) as Q1
                                    , percentile_cont(0.5) within group (order by "{0}") over (partition by null) as Q2
                                    , percentile_cont(0.75) within group (order by "{0}") over (partition by null) as Q3
                                    , percentile_cont(0.90) within group (order by "{0}") over (partition by null) as P90
                                    , percentile_cont(0.95) within group (order by "{0}") over (partition by null) as P95
                                    , percentile_cont(0.975) within group (order by "{0}") over (partition by null) as P975
                                    , percentile_cont(0.99) within group (order by "{0}") over (partition by null) as P99
                                    , percentile_cont(0.75) within group (order by "{0}") over (partition by null) - percentile_cont(0.25) within group (order by "{0}") over (partition by null) as IQR
                            from {1}.{2}.{3}""".format(column_name, table_catalog, table_schema, table_name)
        try:
            cursor_source.execute(sql_percentiles)
            rows = cursor_source.fetchall()
            for row in rows:
                sql_update = """update stats set P01 = ?
                                , P025 = ?
                                , P05 = ?
                                , P10 = ?
                                , Q1  = ?
                                , Q2  = ?
                                , Q3  = ?
                                , P90  = ?
                                , P95  = ?
                                , P975 = ?
                                , P99  = ?
                                , IQR  = ?
                                where SERVER_NAME = ?
                                AND TABLE_CATALOG = ?
                                AND TABLE_SCHEMA = ?
                                AND TABLE_NAME = ?
                                AND COLUMN_NAME = ?;"""
                cursor_metadata.execute(sql_update, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], server_name, table_catalog, table_schema, table_name, column_name))
                conn_metadata.commit()
        except:
            print(sql_percentiles)
            pass
        
        return
    
    columns = getNumericColumnsFromTable(server_name, table_catalog, table_schema, table_name)
    pbar = tqdm(columns)
    for column in pbar:
        pbar.set_description('Column %s' % column[4])
        if checkIfTableExistInStats(server_name, table_catalog, table_schema, table_name, column[4]):
            sql_delete = """delete from stats
                    WHERE SERVER_NAME = ?
                     AND TABLE_CATALOG = ?
                     AND TABLE_SCHEMA = ?
                     AND TABLE_NAME = ?
                     AND COLUMN_NAME = ?;"""
            cursor_metadata.execute(sql_delete, (server_name, table_catalog, table_schema, table_name, column[4]))
            conn_metadata.commit()
            
        if level == 'one':
            insertBasicStats(server_name, table_catalog, table_schema, table_name, column[4])
        elif level == 'two':
            insertBasicStats(server_name, table_catalog, table_schema, table_name, column[4])
            updatePercentiles(server_name, table_catalog, table_schema, table_name, column[4])
        elif level == 'three':
            insertBasicStats(server_name, table_catalog, table_schema, table_name, column[4])
            updatePercentiles(server_name, table_catalog, table_schema, table_name, column[4])
            #updateKurtSkew(server_name, table_catalog, table_schema, table_name, column[4])
        
        if verbose:
            logger.info('{}.{}.{}.{}.{} updated into stats...'.format(server_name, table_catalog, table_schema, table_name, column[4]))
    
    cursor_source.close()
    conn_source.close()

    cursor_metadata.close()
    conn_metadata.close()

    return

def getTablesFromServer(server_name, n_rows_gt = 0):
    """
    Given a server name, it will returns SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, and N_ROWS.
    This list can be used to go over each table and process it.
    """
    conn_metadata = get_db_connection(metadata_connection_params)
    cursor_metadata = conn_metadata.cursor()

    sql = """select distinct SERVER_NAME 
                , TABLE_CATALOG 
                , TABLE_SCHEMA 
                , TABLE_NAME
                , N_ROWS
                from tables
                where SERVER_NAME = ?
                    and N_ROWS > {}
                order by N_ROWS;""".format(n_rows_gt)
    cursor_metadata.execute(sql, (server_name,))
    rows = cursor_metadata.fetchall()

    cursor_metadata.close()
    conn_metadata.close()

    return rows

def fill_columns(server_name):
    conn_source = get_db_connection(source_connection_params)
    cursor_source = get_db_cursor(conn_source)

    print('\n[', colored('OK', 'green'), ']', """\tCollecting data about the:
    \tserver, catalog, database, table names, and column names. 
    \tEach row is a column of a table of the database.\n""")
    sql = """SELECT ? AS SERVER_NAME
            , C.TABLE_CATALOG
            , C.TABLE_SCHEMA
            , C.TABLE_NAME
            , C.COLUMN_NAME
            , C.ORDINAL_POSITION
            , C.DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS AS C INNER JOIN INFORMATION_SCHEMA.TABLES AS T
        ON C.TABLE_CATALOG = T.TABLE_CATALOG
        AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
        AND C.TABLE_NAME = T.TABLE_NAME
        AND T.TABLE_TYPE = 'BASE TABLE';"""

    cursor_source.execute(sql, server_name)
    rows = cursor_source.fetchall()

    cursor_source.close()
    conn_source.close()

    conn_metadata = get_db_connection(metadata_connection_params)
    cursor_metadata = conn_metadata.cursor()

    for row in tqdm(rows, desc = 'Columns'):
        server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type = row
        insertOrUpdateColumns(conn_metadata, cursor_metadata, server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type)
    
    cursor_metadata.close()
    conn_metadata.close()

    return

def fill_tables(server_name):
    print('\n[', colored('OK', 'green'), ']', """\tCollecting number of rows and columns of each table. 
    \tEach row is a table of the database.\n""")

    pbar = tqdm(getColumnsFromServer(server_name))
    for row in pbar:
        pbar.set_description('Table {}'.format(row[3]))
        insertOrUpdateTables(row[0],row[1],row[2],row[3], verbose = False)
    
    return

def fill_uniques(server_name, n_rows_gt = 0):
    print('\n[', colored('OK', 'green'), ']', """\tCollecting the number of NULL values and 
    \tthe number of unique data values. 
    \tEach row represents a column of a table.\n""")
    
    pbar = tqdm(getTablesFromServer(server_name, n_rows_gt))
    for row in pbar:
        pbar.set_description('Table {} {:,} records'.format(row[3], row[4]))
        insertOrUpdateUniques(row[0],row[1],row[2],row[3], verbose = False)
    return

def fill_data_values(server_name, n_rows_gt = 0, with_data_sample = False, n_samples = 10000):
    print('\n[', colored('OK', 'green'), ']', """\tCollecting the frequency count of each data 
    \tvalue of each columns up to a threshould of 5,000 
    \tdistinct values by default.\n""")
    
    pbar = tqdm(getTablesFromServer(server_name, n_rows_gt))
    for row in pbar:
        pbar.set_description('Table {} {:,} records'.format(row[3], row[4]))
        insertOrUpdateDataValues(row[0],row[1],row[2],row[3], verbose = False, with_data_sample=with_data_sample, n_samples=n_samples)
    return

def fill_dates(server_name, n_rows_gt = 0):
    print('\n[', colored('OK', 'green'), ']', """\tCollecting monthly summary of columns of types 
    \t'datetime', 'timestamp', or 'date'\n""")
    
    pbar = tqdm(getTablesFromServer(server_name, n_rows_gt))
    for row in pbar:
        pbar.set_description('Table {} {:,} records'.format(row[3], row[4]))
        insertOrUpdateDates(row[0],row[1],row[2],row[3], verbose = False)
    return

def fill_stats(server_name, n_rows_gt = 0, with_data_sample = False):
    print('\n[', colored('OK', 'green'), ']', """\tCollecting Statistics from the numeric variables.\n""")
    
    pbar = tqdm(getTablesFromServer(server_name, n_rows_gt))
    for row in pbar:
        pbar.set_description('Table {} {:,} records'.format(row[3], row[4]))
        insertOrUpdateStats(row[0],row[1],row[2],row[3], verbose = False, level = 'two')
    return

def describe_server(server_name):
    print('\n[', colored('OK', 'green'), ']', """\tCollecting metadata from {}""".format(server_name))
    fill_columns(server_name)
    fill_tables(server_name)
    fill_uniques(server_name)
    fill_data_values(servr_name)
    fill_dates(server_name)
    fill_stats(server_name)
    return

ignore_columns = ['InsertETLLoadID', 'UpdateETLLoadID']
ignore_tables = ['FleetUtilSummaryOld', 'FleetUtilSummary_Archive2017', 'BPM_PricePerDay_Archive2017']
