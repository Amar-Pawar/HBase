'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-08-12
@Last Modified by: Amar Pawar
@Last Modified time: 2021-08-12
@Title : Program to import realtime stock data in HBase table with happybase
/**********************************************************************************
'''
import happybase as hb
from logging_handler import logger
import csv
from dotenv import load_dotenv
load_dotenv('.env')

def create_hbase_connection():
    """
    Description:
        This function will create connection with hbase.
    """
    try:
        conn = hb.Connection()
        conn.open()
        return conn
    except Exception as e:
        logger.info(e)

def create_table():
    """
    Description:
        This function will create table in hbase with given name and column family.
    """
    try:
        connection = create_hbase_connection()
        connection.create_table('stock_data',{'cf1':dict(max_versions=1),'cf2':dict(max_versions=1),'cf3':dict(max_versions=1),'cf4':dict(max_versions=1),'cf4':dict(max_versions=1),'cf5':dict(max_versions=1)})
        logger.info("Table created")
    except Exception as e:
        logger.info(f"Errorr!!{e}")
        connection.close()

create_hbase_connection()
create_table()