#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf 
from pyspark.sql.functions import * 
from pyspark.sql import DataFrame
from logging import Logger
import os
import json
import psycopg2
import pyodbc 
import cx_Oracle
import pandas as pd
import sys


# In[2]:


import import_ipynb
import db_utils as dbu


# In[3]:


SPARK_CONFIG = {
    "MASTER": "local[*]",
    "settings": {
      "spark.executor.cores": "1",
      "spark.executor.memory": "1g",
      "spark.driver.cores": "1",
      "spark.driver.memory": "1g",
      "spark.cores.max": "1"
    }
}


# In[4]:


def init_spark_context() -> SparkContext:
    """ init spark context """

    # os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars jars/postgresql-42.5.0 pyspark-shell'
    conf = SparkConf()
    conf.setMaster(SPARK_CONFIG['MASTER'])
    conf.setAppName('app')

    for setting, value in SPARK_CONFIG['settings'].items():
        conf.set(setting, value)

    sc = SparkContext(conf=conf)

    return sc

sc = init_spark_context()
spark = SparkSession(sc)


# In[ ]:


# from pyspark.conf import SparkConf
# from pyspark.sql import SparkSession
# spark.sparkContext._conf.getAll()


# In[ ]:


# spark


# In[5]:


def get_source_target_details(pid:int):
    """
    Function to get source and target details from backedn metadeta table and convert them
    into a dict format which is required by driver code
    Args:
        pid: process id or pid to query the details from meta deta table
    returns:
        source_details, target_details dicts for used in driver code
        and mapping file, name of mapping file stored in the mappings folder
    """
    try:
        # create connection to metadata table
        host = '192.168.2.26'
        database = 'postgres'
        user = 'postgres'
        password = 'Fresh*123'
        conn = psycopg2.connect(
                        host=host,
                        database=database,
                        user=user,
                        password=password)
        cur = conn.cursor()

        # queries to get source and target details for metadeta table
        # process id or pid should be give as parameter while invoking this function = 4
        source_details_query = f'''SELECT  
        "SRC_INPUT_TYPE", 
        "SRC_QUERY", 
        "SRC_TABLE", 
        "SRC_CONNECTION_NAME",
        "SRC_CONNECTION_TYPE",
        "SRC_DATABASE_DRIVER",
        (case when ("SRC_DATABASE_NAME" IS NOT NULL) then "SRC_DATABASE_NAME"
        else 'delimited_file' end ) as "SRC_DATABASE_NAME",
        "SRC_JDBCURL",
        "SRC_USER_NAME", 
        "SRC_PASSWORD",
        "SRC_FILE_PATH", 
        "SRC_FILE_NAME", 
        "SRC_DELIMITER"

        FROM semarchy_data_migration_utility.dbv_get_process_vw
        where "PROCESS_ID" = {pid}; 
        '''

        target_details_query = f'''SELECT  
        "TRG_TABLE",
        "TRG_CONNECTION_NAME", 
        "TRG_CONNECTION_TYPE", 
        "TRG_DATABASE_DRIVER", 
        (case when ("TRG_DATABASE_NAME" IS NOT NULL) then "TRG_DATABASE_NAME"
        else 'delimited_file' end ) as "TRG_DATABASE_NAME", 
        "TRG_JDBCURL", 
        "TRG_USER_NAME", 
        "TRG_PASSWORD", 
        "TRG_FILE_PATH", 
        "TRG_FILE_NAME", 
        "TRG_DELIMITER", 
        "MAPPING_NAME",
        "TRG_WRITE_MODE"
        FROM semarchy_data_migration_utility.dbv_get_process_vw
        where "PROCESS_ID" = {pid}; 
        '''

        # read the queries in pandas df
        source_details_df = pd.read_sql(source_details_query, conn)
        target_details_df = pd.read_sql(target_details_query, conn)

        # create dictionary out of pandas dfs
        source_dict = source_details_df.to_dict()
        source_dict2 = {key:value[0] for key,value in source_dict.items() }

        target_dict = target_details_df.to_dict()
        target_dict2 = {key:value[0] for key,value in target_dict.items() }

        # source and target config templated to be passed for driver code 
        tmp_src = {
        "source_name" : "SRC_DATABASE_NAME",
        "source_config" : {
                        "url" : "SRC_JDBCURL",
                        "driver" : "SRC_DATABASE_DRIVER",
                        "user" : "SRC_USER_NAME",
                        "password" : "SRC_PASSWORD"},
        "source_query" : "SRC_QUERY",
        "source_table" : "SRC_TABLE",
        "source_filename" : "SRC_FILE_NAME",
        "source_delimiter" : "SRC_DELIMITER"
        }

        tmp_trg = {
            "target_name" : "TRG_DATABASE_NAME",
            "target_config" : {
                            "url" : "TRG_JDBCURL" ,
                            "driver" : "TRG_DATABASE_DRIVER",
                            "user" : "TRG_USER_NAME",
                            "password" : "TRG_PASSWORD"
                            },
            "target_table" : "TRG_TABLE",
            "target_filename" : "TRG_FILE_NAME",
            "target_delimiter" : "TRG_DELIMITER",
            "target_write_mode" : "TRG_WRITE_MODE" 
        }

        # logic to convert the dicts in required format 
        source_details = {}
        source_config = {}
        for key,value in source_dict2.items():
            for k,v in tmp_src['source_config'].items():
                if key == v:
                    source_config[k] = value

        for key,value in source_dict2.items():
            source_details['source_config'] = source_config
            for k,v in tmp_src.items():
                if key == v:
                    source_details[k] = value

        print('source_details == ', source_details )


        target_details = {}
        target_config = {}
        for key,value in target_dict2.items():
            for k,v in tmp_trg['target_config'].items():
                if key == v:
                    target_config[k] = value

        for key,value in target_dict2.items():
            target_details['target_config'] = target_config
            for k,v in tmp_trg.items():
                if key == v:
                    target_details[k] = value

        print('target_details == ', target_details )
        mapping_fileName = target_dict2['MAPPING_NAME']
        print('Mapping_file Name = ',mapping_fileName)
        return source_details, target_details, mapping_fileName
    
    except Exception as e:
        print("Failure occured check logs")
        print(f"{e}")
        return None


# In[ ]:





# In[6]:


def driver_code(spark:SparkSession, source_info:dict, target_info:dict, mapping_filename:str):
    """
    This is the driver code function
    the flow of execution based on type of source and target gets executed inside this function
    Args:
        Function takes two dicts as arugmnets which as source and target information
        a sample of these inputs is :
            source_config = {
                        "source_name" : "",
                        "source_config" : {
                                        "url" : "",
                                        "driver" : "",
                                        "user" : "",
                                        "password" : ""},
                        "source_query" : "",
                        "source_table" : "",
                        "source_filename" : "",
                        "source_delimiter" : ""
                    }

                    target_config = {

                        "target_name" : "",
                        "target_config" : {
                                        "url" : "",
                                        "driver" : "",
                                        "user" : "",
                                        "password" : ""
                                        },
                        "target_table" : "",
                        "target_filename" : "",
                        "target_delimiter" : "",
                        "target_write_mode" : "" 
                    }
    """
    source_name = source_info['source_name'].lower()
    source_config = source_info['source_config']
    source_query = source_info['source_query']
    source_table = source_info['source_table']
    source_filename = source_info['source_filename']
    source_delimiter = source_info['source_delimiter']
    
    target_name = target_info['target_name'].lower()
    target_config = target_info['target_config'] 
    target_table = target_info['target_table']
    target_filename = target_info['target_filename'] 
    target_delimiter = target_info['target_delimiter']
    target_write_mode = target_info['target_write_mode'].lower()
    
    try:
        # reading from source into source_df
        if source_name == 'oracle':
            source_df = dbu.read_from_oracle(spark , source_config, source_query, source_table)

        elif source_name == 'sqlserver':
            source_df = dbu.read_from_msssql(spark , source_config, source_query, source_table)

        elif source_name == 'postgres':
            source_df = dbu.read_from_pg(spark , source_config, source_query, source_table)

        elif source_name == 'delimited_file':
            source_df = dbu.read_csv_file(spark , source_filename, source_delimiter)  
            
            
        print('Data read from source')
        
        if target_name != 'delimited_file':
            
            #reading from target for target_df creation
            if target_name == 'oracle':
                target_df = dbu.read_from_oracle(spark , target_config, '', target_table)

            elif target_name == 'sqlserver':
                target_df = dbu.read_from_msssql(spark , target_config, '', target_table)

            elif target_name == 'postgres':
                target_df = dbu.read_from_pg(spark , target_config, '', target_table)


            # generating mappings
            mappings = dbu.mapping_generation(spark, mapping_filename)
            columns_for_date_conversion = mappings['columns_for_date_conversion']
            source_to_target_mapping = mappings['source_target_column_mapping']
            static_target_columns = mappings['static_target_columns']
            default_value_for_null_columns = mappings['default_value_for_null_columns']

            # converting primary data types 
            print("****converting preliminary data types****")
            print("columns converted: ")
            type_converted_df = dbu.convert_to_target_dtypes(source_df, target_df, source_to_target_mapping)

            #converting date column types and formats
            print("****converting date column types and formats****")
            # print(columns_for_date_conversion)
            date_converted_df = dbu.date_column_format_converter(type_converted_df ,columns_for_date_conversion)

            #adding values for hard coded columns
            print("****adding values for hard coded columns****")
            print("columns converted: ")
            hard_coded_value_populated_df = dbu.populate_column_with_default_values(date_converted_df, static_target_columns)

            #adding default values for nulls
            print("****adding default values for nulls****")
            fill_na_dict = dbu.create_fill_na_dict(default_value_for_null_columns)
            fill_na_dict = {k.lower(): v for k, v in fill_na_dict.items()}
            print("\tcolumns with default values for NULL:", fill_na_dict)
            null_populated_df = dbu.populate_null_values(hard_coded_value_populated_df, fill_na_dict)

            #write to target
            print("****Writing to target") 
            columns_list = list(set(target_df.columns).intersection(null_populated_df.columns)) 
            final_df = null_populated_df.select(*columns_list)
            if target_name == 'oracle':
                dbu.write_to_oracle(spark, final_df, target_config, target_table, target_write_mode) 

            elif target_name == 'sqlserver':
                dbu.write_to_mssql(spark, final_df, target_config, target_table, target_write_mode) 

            elif target_name == 'postgres':
                dbu.write_to_pg(spark, final_df, target_config, target_table, target_write_mode) 

        else:
            ## Need to add function to convert output for delimited_files
            dbu.write_to_csv(source_df, target_filename, target_delimiter, target_write_mode)
        
        print('Process completed')
        
    except Exception as e:
        print("Failure occured check logs")
        print(f"{e}")
        return None



if __name__ == "__main__":
    pid = int(sys.argv[1])
    source_info,target_info,mapping_filename = get_source_target_details(pid)

    driver_code(spark, source_info, target_info, mapping_filename)

    spark.stop()
