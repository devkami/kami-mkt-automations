#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from os import getenv, system, path
from typing import Dict, List

import pandas as pd
from constant import (
    BILLINGS_DATETIME_COLS,
    CUSTOMER_DETAILS_DATETIME_COLS,
    CUSTOMER_DETAILS_NUM_COLS,
    CUSTOMER_DETAILS_SCRIPT,
    DAILY_BILLINGS_NUM_COLS,
    DAILY_BILLINGS_SCRIPT,
    MONTHLY_BILLINGS_NUM_COLS,
    MONTHLY_BILLINGS_SCRIPT,
    FUTURE_BILLS_SCRIPT,
    FUTURE_BILLS_DATETIME_COLS,
    FUTURE_BILLS_NUM_COLS
)
from dotenv import load_dotenv
from filemanager import get_file_list_from
from kami_logging import benchmark_with, logging_with
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import URL, Engine

db_connector_logger = logging.getLogger('database')
load_dotenv()
connection_url = URL.create(
    'mysql+pymysql',
    username=getenv('DB_USER'),
    password=getenv('DB_USER_PASSWORD'),
    host=getenv('DB_HOST'),
    database='db_uc_kami',
)

@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def create_and_connect_engine() -> Engine|None:
    sql_engine = None
    try:
        sql_engine = create_engine(connection_url, pool_recycle=3600)
        sql_engine.connect()
    except SQLAlchemyError as e:
        db_connector_logger.exception("The following error was generated when trying to create a database connection through sqlalchemy:", e)
        raise e     
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e) 
        raise e
    return sql_engine

@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def execute_query(sql_file):
    sql_command = f"mysql -u {getenv('DB_USER')} -p{getenv('DB_USER_PASSWORD')} -h {getenv('DB_HOST')} -P {getenv('DB_PORT')} < {sql_file}"
    try:
        db_connector_logger.info(f'execute {sql_file}')
        system(sql_command)        
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e) 
        raise e
        


@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def execute_queries(sql_files):
    try:
        sql_files.sort()
        if len(sql_files):
            for sql_file in sql_files:
                execute_query(sql_file)
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e) 
        raise e


@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def update_database_views():
    try:
        views_script = get_file_list_from('data/in')
        execute_queries(views_script)
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e) 
        raise e


@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def get_dataframe_from_sql_query(
    sql_script: str, date_cols: List[str] = [], cols_types: Dict = {}
) -> pd.DataFrame | None:
    df = None
    sql_engine = None
    try:
        sql_engine = create_and_connect_engine()
        if sql_engine:
            df = pd.read_sql_query(
                str(sql_script),
                sql_engine,
                parse_dates=date_cols,
                dtype=cols_types
            )
        else:
            raise ValueError("sql_engine is not properly initialized.")        
    except Exception as e:
        db_connector_logger.exception("Error executing SQL query.", e)
        if sql_engine:
            sql_engine.dispose()
        raise e
    finally:
        if sql_engine:
            sql_engine.dispose()  
    return pd.DataFrame(df)

@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def get_dataframe_from_sql_file(
    sql_file: str, date_cols: List[str] = [], cols_types: Dict = {}
) -> pd.DataFrame | None:
    df = None
    query_file = None
    try:
        query_file = open(sql_file, 'r')
        sql_engine = create_and_connect_engine()  
        df = pd.read_sql(
            query_file.read(), sql_engine, parse_dates=date_cols, dtype=cols_types
        )
        
    except FileNotFoundError as e:
        db_connector_logger.exception(f"SQL file {sql_file} not found.", e)
    except Exception as e:
        db_connector_logger.exception("An error occurred while reading the SQL file or executing the query.", e)
    finally:
        if 'query_file' in locals() and query_file:
            query_file.close()
    return df

@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def get_dataframe_from_sql(
    sql_query: str, date_cols: List[str] = [], cols_types: Dict = {}
) -> pd.DataFrame | None:
    df = None
    try:
        if path.exists(sql_query):
            df = get_dataframe_from_sql_file(sql_query, date_cols, cols_types)
        df = get_dataframe_from_sql_query(sql_query, date_cols, cols_types)
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e)         
    return df  


@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def get_dataframe_from_sql_table(
    tablename: str, date_cols: List[str] = []
) -> pd.DataFrame | None:
    df = None
    try:
        sql_engine = create_and_connect_engine()
        df = pd.read_sql_table(tablename, sql_engine, parse_dates=date_cols)
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e)         
    return df  


@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def get_vw_monthly_billings() -> pd.DataFrame | None:
    df = None
    try:      
        df = get_dataframe_from_sql_query(
            MONTHLY_BILLINGS_SCRIPT,
            date_cols=BILLINGS_DATETIME_COLS,
            cols_types=MONTHLY_BILLINGS_NUM_COLS,
        )
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e)         
    return df
    


@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def get_vw_daily_billings() -> pd.DataFrame | None:
    df = None
    try:      
        df = get_dataframe_from_sql_query(
            DAILY_BILLINGS_SCRIPT,
            date_cols=BILLINGS_DATETIME_COLS,
            cols_types=DAILY_BILLINGS_NUM_COLS,
        )
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e)         
    return df
    


@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def get_vw_customer_details() -> pd.DataFrame | None:
    df = None
    try:      
        df = get_dataframe_from_sql_query(
            CUSTOMER_DETAILS_SCRIPT,
            date_cols=CUSTOMER_DETAILS_DATETIME_COLS,
            cols_types=CUSTOMER_DETAILS_NUM_COLS,
        )
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e)         
    return df
    

@benchmark_with(db_connector_logger)
@logging_with(db_connector_logger)
def get_vw_future_bills() -> pd.DataFrame | None:
    df = None
    try:      
        df = get_dataframe_from_sql_query(
            FUTURE_BILLS_SCRIPT,
            date_cols=FUTURE_BILLS_DATETIME_COLS,
            cols_types=FUTURE_BILLS_NUM_COLS,
        )
    except Exception as e:
        db_connector_logger.exception("An unknow error occurred:", e)         
    return df
