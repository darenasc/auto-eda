from __future__ import division
import pymssql
import pyodbc
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import sqlite3

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger(__name__)

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
    
    sql_summary = '''CREATE TABLE IF NOT EXISTS summary (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , ORDINAL_POSITION INTEGER
            , DATA_TYPE TEXT)'''

    sql_summary_v2 = '''CREATE TABLE IF NOT EXISTS summary_v2 (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , N_COLUMNS INTEGER
            , N_ROWS INTEGER)'''

    sql_summary_v3 = '''CREATE TABLE IF NOT EXISTS summary_v3 (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , ORDINAL_POSITION INTEGER
            , DATA_TYPE TEXT
            , DISTINCT_VALUES INTEGER
            , NULL_VALUES INTEGER)'''

    sql_summary_v4 = '''CREATE TABLE IF NOT EXISTS summary_v4 (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , DATA_VALUE TEXT
            , FREQUENCY_NUMBER INTEGER
            , FREQUENCY_PERCENTAGE FLOAT)'''

    sql_summary_v5 = '''CREATE TABLE IF NOT EXISTS summary_v5 (SERVER_NAME TEXT
            , TABLE_CATALOG TEXT
            , TABLE_SCHEMA TEXT
            , TABLE_NAME TEXT
            , COLUMN_NAME TEXT
            , DATA_VALUE TEXT
            , FREQUENCY_NUMBER INTEGER
            , FREQUENCY_PERCENTAGE FLOAT)'''
    
    sql_summary_v6 = '''CREATE TABLE IF NOT EXISTS summary_v6 (SERVER_NAME TEXT
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
    
    cursor.execute(sql_summary)
    cursor.execute(sql_summary_v2)
    cursor.execute(sql_summary_v3)
    cursor.execute(sql_summary_v4)
    cursor.execute(sql_summary_v5)
    cursor.execute(sql_summary_v6)
    
    db.commit()
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

def insertOrUpdateSummary(server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, verbose= False):
    def checkIfTableExistInSummary(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """select * from summary
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?
             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name))
        return len(cursor_metadata.fetchall())
    
    if checkIfTableExistInSummary(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """delete from summary
                WHERE SERVER_NAME = ?
                 AND TABLE_CATALOG = ?
                 AND TABLE_SCHEMA = ?
                 AND TABLE_NAME = ?
                 AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name))
        conn_metadata.commit()
    
    sql = """insert into summary(SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE)
             values (?, ?, ?, ?, ?, ?, ?)"""
    cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type))
    conn_metadata.commit()
    if verbose:
        logger.info('{}.{}.{}.{}.{} has been updated into summary...'.format(server_name, table_catalog, table_schema, table_name, column_name))
    return

def insertOrUpdateSummaryV2(server_name, table_catalog, table_schema, table_name, verbose = False, ignore_views = True):
    """
    Stores the number of columns and the number of rows of the table.
    Each row is one table.
    """
    def checkIfTableExistInSummaryV2(server_name, table_catalog, table_schema, table_name):
        sql = """select * from summary_v2
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

        sql_update = """UPDATE summary_v2 
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
        sql_insert = """insert into summary_v2 (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, N_COLUMNS, N_ROWS)
                        values (?, ?, ?, ?, ?, ?);"""
        for row in rows:
            cursor_metadata.execute(sql_insert, (row[0], row[1], row[2], row[3], row[4], row[5]))
            conn_metadata.commit()
        return
    
    if checkIfTableExistInSummaryV2:
        sql = """delete from summary_v2
                WHERE SERVER_NAME = ?
                 AND TABLE_CATALOG = ?
                 AND TABLE_SCHEMA = ?
                 AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name))
        conn_metadata.commit()
        
    updateNumberOfColumns(server_name, table_catalog, table_schema, table_name)
    updateNumberOfRows(server_name, table_catalog, table_schema, table_name)
        
    if verbose:
        logger.info('{}.{}.{}.{} updated into summary_v2...'.format(server_name, table_catalog, table_schema, table_name))
        
    return

def insertOrUpdateSummaryV3(server_name, table_catalog, table_schema, table_name, verbose = False):
    def checkIfTableExistInSummaryV3(server_name, table_catalog, table_schema, table_name):
        sql = """select * from summary_v3
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name))
        return len(cursor_metadata.fetchall())
    
    def getColumnsFromTable(server_name, table_catalog, table_schema, table_name):
        sql_fields = """select column_name
                            , ORDINAL_POSITION
                            , DATA_TYPE 
                        from summary 
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql_fields, (server_name, table_catalog, table_schema, table_name))
        return cursor_metadata.fetchall()
    
    def getValuesFromColumn(server_name, table_catalog, table_schema, table_name, column_name):
        sql_values = """select count(distinct "{}") as distinctValues
                                , sum(case when "{}" is null then 1 else 0 end) as nullValues
                        FROM    {}.{}""".format(column_name, column_name, table_schema, table_name)
        cursor_source.execute(sql_values)
        return cursor_source.fetchall()
    
    def insertValuesInSummaryV3(server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, distinctValues, nullValues):
        sql_insert = """insert into summary_v3 (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, DISTINCT_VALUES, NULL_VALUES)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, distinctValues, nullValues))
        conn_metadata.commit()
        return
    
    if checkIfTableExistInSummaryV3(server_name, table_catalog, table_schema, table_name):
        sql = """delete from summary_v3
                WHERE SERVER_NAME = ?
                 AND TABLE_CATALOG = ?
                 AND TABLE_SCHEMA = ?
                 AND TABLE_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name))
        conn_metadata.commit()
        
    columns = getColumnsFromTable(server_name, table_catalog, table_schema, table_name)
    
    for field in columns:
        if field[2] not in ('text', 'image', 'ntext'):
        #if field[2] != 'text' and field[2] != 'image' and field[2] != 'ntext':
            values = getValuesFromColumn(server_name, table_catalog, table_schema, table_name, field[0])
            insertValuesInSummaryV3(server_name, table_catalog, table_schema, table_name, field[0], field[1], field[2], values[0][0], values[0][1])
            
        if verbose:
            logger.info('{}.{}.{}.{}.{} updated into summary_v3...'.format(server_name, table_catalog, table_schema, table_name, field[0]))
            
    return

def insertOrUpdateSummaryV4(server_name, table_catalog, table_schema, table_name, verbose = False, threshold = 5000):
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
    def checkIfTableExistInSummaryV4(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """select * from summary_v4
            WHERE SERVER_NAME = ?
             AND TABLE_CATALOG = ?
             AND TABLE_SCHEMA = ?
             AND TABLE_NAME = ?
             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql, (server_name, table_catalog, table_schema, table_name, column_name))
        return len(cursor_metadata.fetchall())
    
    def getColumnsFromTable(server_name, table_catalog, table_schema, table_name):
        sql_fields = """select server_name
                            , table_catalog
                            , table_schema
                            , table_name
                            , column_name
                        from summary 
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND DATA_TYPE NOT IN ('text', 'image', 'ntext');"""
        cursor_metadata.execute(sql_fields, (server_name, table_catalog, table_schema, table_name))
        return cursor_metadata.fetchall()
    
    def insertFrequencyValue(server_name, table_catalog, table_schema, table_name, column_name, threshold):
        num_distinct_values = getNumDistinctValues(server_name, table_catalog, table_schema, table_name, column_name)
        
        if num_distinct_values < threshold:
            sql_frequency = """SELECT [{}]
                                    , COUNT(*) AS N 
                                FROM {}.{}.{} 
                                GROUP BY [{}] 
                                ORDER BY N DESC;""".format(column_name, table_catalog, table_schema, table_name, column_name)
            cursor_source.execute(sql_frequency)
            rows = cursor_source.fetchall()

            for row in rows:
                sql_insert = """insert into summary_v4 (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                                values (?, ?, ?, ?, ?, ?, ?);"""
                if isinstance(row[0], str):
                    cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, row[0], row[1]))
                else:
                    cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, str(row[0]), row[1]))
                conn_metadata.commit()
        return
    
    def insertFrequencyPercentage(server_name, table_catalog, table_schema, table_name, column_name):
        sql_total = """SELECT SUM(FREQUENCY_NUMBER) AS TOTAL
                        FROM SUMMARY_V4
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_total, (server_name, table_catalog, table_schema, table_name, column_name))
        total = cursor_metadata.fetchall()[0][0]
        
        sql_frequency = """SELECT DATA_VALUE, FREQUENCY_NUMBER
                            FROM SUMMARY_V4
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_frequency, (server_name, table_catalog, table_schema, table_name, column_name))
        rows = cursor_metadata.fetchall()
        for row in rows:
            sql_update = """UPDATE summary_v4 SET FREQUENCY_PERCENTAGE = ?
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?
                             AND DATA_VALUE = ?;"""
            cursor_metadata.execute(sql_update, ((row[1] / total), server_name, table_catalog, table_schema, table_name, column_name, row[0]))
            conn_metadata.commit()        
        return
    
    def getNumDistinctValues(server_name, table_catalog, table_schema, table_name, column_name):
        sql_check_threshold = """select DISTINCT_VALUES from summary_v3 
                                 where SERVER_NAME = ?
                                     AND TABLE_CATALOG = ?
                                     AND TABLE_SCHEMA = ?
                                     AND TABLE_NAME = ?
                                     AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_check_threshold, (server_name, table_catalog, table_schema, table_name, column_name))
        rows = cursor_metadata.fetchone()
        return rows[0]
    
    columns = getColumnsFromTable(server_name, table_catalog, table_schema, table_name)
    
    for column in columns:
        if checkIfTableExistInSummaryV4(server_name, table_catalog, table_schema, table_name, column[4]):
            sql_delete = """delete from summary_v4
                    WHERE SERVER_NAME = ?
                     AND TABLE_CATALOG = ?
                     AND TABLE_SCHEMA = ?
                     AND TABLE_NAME = ?
                     AND COLUMN_NAME = ?;"""
            cursor_metadata.execute(sql_delete, (server_name, table_catalog, table_schema, table_name, column[4]))
            conn_metadata.commit()
        
        insertFrequencyValue(server_name, table_catalog, table_schema, table_name, column[4], threshold)
        insertFrequencyPercentage(server_name, table_catalog, table_schema, table_name, column[4])
        
        if verbose:
            logger.info('{}.{}.{}.{}.{} updated into summary_v4...'.format(server_name, table_catalog, table_schema, table_name, column[4]))
    
    return

def insertOrUpdateSummaryV5(server_name, table_catalog, table_schema, table_name, verbose = False, thresold = 5000):
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
    def checkIfTableExistInSummaryV5(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """select * from summary_v5
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
                            from summary 
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND DATA_TYPE IN ('datetime', 'timestamp', 'date');"""
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
        cursor_source.execute(sql_agg_month)
        rows = cursor_source.fetchall()
        if len(rows) < thresold:
            for row in rows:
                sql_insert = """INSERT INTO SUMMARY_V5 (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_VALUE, FREQUENCY_NUMBER)
                                VALUES (?, ?, ?, ?, ?, ?, ?)"""
                cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, row[0], row[1]))
                conn_metadata.commit()
        return
    
    def updateFrequencyPercentage(server_name, table_catalog, table_schema, table_name, column_name):
        sql_total = """SELECT SUM(FREQUENCY_NUMBER) AS TOTAL
                        FROM SUMMARY_V5
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_total, (server_name, table_catalog, table_schema, table_name, column_name))
        total = cursor_metadata.fetchall()[0][0]
        
        sql_frequency = """SELECT DATA_VALUE, FREQUENCY_NUMBER
                            FROM SUMMARY_V5
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?;"""
        cursor_metadata.execute(sql_frequency, (server_name, table_catalog, table_schema, table_name, column_name))
        rows = cursor_metadata.fetchall()
        for row in rows:
            sql_update = """UPDATE summary_v5 SET FREQUENCY_PERCENTAGE = ?
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
    for column in columns:
        if checkIfTableExistInSummaryV5(server_name, table_catalog, table_schema, table_name, column[4]) > 0:
            sql_delete = """delete from summary_v5
                            WHERE SERVER_NAME = ?
                             AND TABLE_CATALOG = ?
                             AND TABLE_SCHEMA = ?
                             AND TABLE_NAME = ?
                             AND COLUMN_NAME = ?;"""
            cursor_metadata.execute(sql_delete, (server_name, table_catalog, table_schema, table_name, column[4]))
            conn_metadata.commit()
        
        insertDateFrequency(server_name, table_catalog, table_schema, table_name, column[4], thresold)
        updateFrequencyPercentage(server_name, table_catalog, table_schema, table_name, column[4])
    
        if verbose:
            logger.info('{}.{}.{}.{}.{} updated into summary_v5...'.format(server_name, table_catalog, table_schema, table_name, column[4]))
    
    return

def insertOrUpdateSummaryV6(server_name, table_catalog, table_schema, table_name, verbose = False, level = 'one'):
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
    def checkIfTableExistInSummaryV6(server_name, table_catalog, table_schema, table_name, column_name):
        sql = """select * from summary_v6
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
                        from summary 
                        WHERE SERVER_NAME = ?
                         AND TABLE_CATALOG = ?
                         AND TABLE_SCHEMA = ?
                         AND TABLE_NAME = ?
                         AND DATA_TYPE IN ('int', 'decimal', 'numeric', 'float', 'money', 'tinyint', 'bigint', 'smallint', 'real');"""
        cursor_metadata.execute(sql_fields, (server_name, table_catalog, table_schema, table_name))
        return cursor_metadata.fetchall()
    
    def insertBasicStats(server_name, table_catalog, table_schema, table_name, column_name):
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
            sql_insert = """insert into summary_v6 (SERVER_NAME, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, AVG, STDEV, VAR, SUM, MAX, MIN, RANGE)
                            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            cursor_metadata.execute(sql_insert, (server_name, table_catalog, table_schema, table_name, column_name, row[0], row[1], row[2], row[3], row[4], row[5], row[6]))
            conn_metadata.commit()
        return
    
    def updatePercentiles(server_name, table_catalog, table_schema, table_name, column_name):
        sql_percentiles = """select distinct 
                                    percentile_cont(0.01) within group (order by {0}) over (partition by null) as P01
                                    , percentile_cont(0.025) within group (order by {0}) over (partition by null) as P025
                                    , percentile_cont(0.05) within group (order by {0}) over (partition by null) as P05
                                    , percentile_cont(0.10) within group (order by {0}) over (partition by null) as P10
                                    , percentile_cont(0.25) within group (order by {0}) over (partition by null) as Q1
                                    , percentile_cont(0.5) within group (order by {0}) over (partition by null) as Q2
                                    , percentile_cont(0.75) within group (order by {0}) over (partition by null) as Q3
                                    , percentile_cont(0.90) within group (order by {0}) over (partition by null) as P90
                                    , percentile_cont(0.95) within group (order by {0}) over (partition by null) as P95
                                    , percentile_cont(0.975) within group (order by {0}) over (partition by null) as P975
                                    , percentile_cont(0.99) within group (order by {0}) over (partition by null) as P99
                                    , percentile_cont(0.75) within group (order by {0}) over (partition by null) - percentile_cont(0.25) within group (order by {0}) over (partition by null) as IQR
                            from {1}.{2}.{3}""".format(column_name, table_catalog, table_schema, table_name)
        cursor_source.execute(sql_percentiles)
        rows = cursor_source.fetchall()
        for row in rows:
            sql_update = """update summary_v6 set P01 = ?
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
        return
    
    columns = getNumericColumnsFromTable(server_name, table_catalog, table_schema, table_name)
    
    for column in columns:
        if checkIfTableExistInSummaryV6(server_name, table_catalog, table_schema, table_name, column[4]):
            sql_delete = """delete from summary_v6
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
            logger.info('{}.{}.{}.{}.{} updated into summary_v6...'.format(server_name, table_catalog, table_schema, table_name, column[4]))
    
    return

def fill_summary(server_name):
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

    for row in rows:
        server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type = row
        insertOrUpdateSummary(server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type)
    return

def fill_summary_v2(server_name):
    sql = """select distinct SERVER_NAME 
                , TABLE_CATALOG 
                , TABLE_SCHEMA 
                , TABLE_NAME
                from summary
                where SERVER_NAME = ?;"""
    cursor_metadata.execute(sql, (server_name,))
    rows = cursor_metadata.fetchall()
    for row in rows:
        insertOrUpdateSummaryV2(row[0],row[1],row[2],row[3], verbose = True)
    return

def fill_summary_v3(server_name):
    sql = """select distinct SERVER_NAME 
                , TABLE_CATALOG 
                , TABLE_SCHEMA 
                , TABLE_NAME
                from summary_v2
                where SERVER_NAME = ?
                order by N_ROWS;"""
    cursor_metadata.execute(sql, (server_name,))
    rows = cursor_metadata.fetchall()
    for row in rows:
        insertOrUpdateSummaryV3(row[0],row[1],row[2],row[3], verbose = True)
    return

def fill_summary_v4(server_name):
    sql = """select distinct SERVER_NAME 
                , TABLE_CATALOG 
                , TABLE_SCHEMA 
                , TABLE_NAME
                from summary_v2
                where SERVER_NAME = ?
                order by N_ROWS;"""
    cursor_metadata.execute(sql, (server_name,))
    rows = cursor_metadata.fetchall()
    for row in rows:
        insertOrUpdateSummaryV4(row[0],row[1],row[2],row[3], verbose = True)
    return

def fill_summary_v5(server_name):
    sql = """select distinct SERVER_NAME 
                , TABLE_CATALOG 
                , TABLE_SCHEMA 
                , TABLE_NAME
                from summary_v2
                where SERVER_NAME = ?
                order by N_ROWS;"""
    cursor_metadata.execute(sql, (server_name,))
    rows = cursor_metadata.fetchall()
    for row in rows:
        insertOrUpdateSummaryV5(row[0],row[1],row[2],row[3], verbose = True)
    return

def fill_summary_v6(server_name):
    sql = """select distinct SERVER_NAME 
                , TABLE_CATALOG 
                , TABLE_SCHEMA 
                , TABLE_NAME
                from summary_v2
                where SERVER_NAME = ?
                order by N_ROWS;"""
    cursor_metadata.execute(sql, (server_name,))
    rows = cursor_metadata.fetchall()
    for row in rows:
        insertOrUpdateSummaryV6(row[0],row[1],row[2],row[3], verbose = True)
    return

def describe_server(server_name):
    %time fill_summary(server_name) 
    %time fill_summary_v2(server_name)
    %time fill_summary_v3(server_name) 
    %time fill_summary_v4(server_name) 
    %time fill_summary_v5(server_name)
    %time fill_summary_v6(server_name)
    return

def describe_table(server_name, table_catalog, table_schema, table_name, verbose = False):
    #%time insertOrUpdateSummary(server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type)
    #%time insertOrUpdateSummaryV2(server_name, table_catalog, table_schema, table_name, verbose = verbose)
    insertOrUpdateSummaryV3(server_name, table_catalog, table_schema, table_name, verbose = verbose)
    insertOrUpdateSummaryV4(server_name, table_catalog, table_schema, table_name, verbose = verbose)
    insertOrUpdateSummaryV5(server_name, table_catalog, table_schema, table_name, verbose = verbose)
    insertOrUpdateSummaryV6(server_name, table_catalog, table_schema, table_name, verbose = verbose)
    return

def describe_database(server_name, table_catalog, table_schema, verbose = False):
    def fill_summary_database(server_name, table_catalog, table_schema):
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
            AND T.TABLE_TYPE = 'BASE TABLE'
            AND T.TABLE_SCHEMA = ?
            AND T.TABLE_CATALOG = ?;"""

        cursor_source.execute(sql, (server_name, table_catalog, table_schema))
        rows = cursor_source.fetchall()

        for row in rows:
            server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type = row
            insertOrUpdateSummary(server_name, table_catalog, table_schema, table_name, column_name, ordinal_position, data_type)
        return

    sql = """select distinct SERVER_NAME 
                , TABLE_CATALOG 
                , TABLE_SCHEMA 
                , TABLE_NAME
                from summary_v2
                where SERVER_NAME = ?
                  and TABLE_CATALOG = ?
                  and TABLE_SCHEMA = ?
                order by N_ROWS;"""
    cursor_metadata.execute(sql, (server_name, table_catalog, table_schema))
    rows = cursor_metadata.fetchall()
    
    fill_summary_v3(server_name) 
    fill_summary_v4(server_name)
    fill_summary_v5(server_name)
    fill_summary_v6(server_name)

    #fill_summary_database(server_name, table_catalog, table_schema)
    """for row in rows:
        describe_table(row[0],row[1],row[2],row[3], verbose = False)
        
        if verbose:
            logger.info('{}.{}.{}.{} updated...'.format(row[0],row[1],row[2],row[3]))"""
    return