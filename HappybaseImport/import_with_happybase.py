'''
/**********************************************************************************
@Author: Amar Pawar
@Date: 2021-08-07
@Last Modified by: Amar Pawar
@Last Modified time: 2021-08-07
@Title : Program to import data from given file to HBase table with happybase
/**********************************************************************************
'''
import happybase as hb
from logging_handler import logger
import csv

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
        connection.create_table('wordcount',{'cf1':dict(max_versions=1),'cf2':dict(max_versions=1)})
        logger.info("Table created")
    except Exception as e:
        logger.info(f"Errorr!!{e}")
        connection.close()

def import_into_hbase():
    """
    Description:
        This function will import data from given file in HBase table the we created.
    """
    try:
        connection=create_hbase_connection()
        table = connection.table('wordcount')
        input_text = csv.DictReader(open("/home/ubuntu/Documents/HadoopWorkspace/HBase/HappybaseImport/wordcount_text"))
        for row in input_text:
            table.put(row['no'],
            {'cf1:word': row['word'],
            'cf2:count':row['count']})
    except Exception as e:
        logger.info(f"Error!!{e}")
        connection.close()

def scan_table():
    """
    Description:
        This function will scan the table in HBase and print the data.
    """
    try:
        connection = create_hbase_connection()
        table = connection.table('wordcount')
        for key,data in table.scan():
            no = key.decode('utf-8')
            for value1, value2 in data.items():
                cf1 = value1.decode('utf-8')
                cf2 = value2.decode('utf-8')
                logger.info(no,cf1,cf2)
    except Exception as e:
        logger.info(e)
    
create_hbase_connection()
create_table()
import_into_hbase()
scan_table()