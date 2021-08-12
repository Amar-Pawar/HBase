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

create_hbase_connection()