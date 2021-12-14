import pandas as pd 
import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

meta_list = []
Database = ""
for table in sqlContext.tableNames(Database):
  
  metadata_df = spark.sql(f"show create table {Database}.{table}")
  metadata_df= metadata_df\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', "CREATE TABLE","CREATE EXTERNAL TABLE"))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', "spark_catalog`.",""))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', 'STRING',"varchar(4000)"))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', 'DOUBLE',"REAL"))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', 'dbfs:/mnt/adls_deloitte_dl/',""))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', 'LOCATION ',"LOCATION ="))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', '`',""))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', 'PARTITIONED BY \(File_Year, File_Month\)',""))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', f'USING delta',"\nWITH ( DATA_SOURCE = [{>add storage endpoint here<}],\nFILE_FORMAT = [SynapseParquetFormat],"))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', "\'$'","\/\*\/\*\/\.parquet"))\
  .withColumn("createtab_stmt",regexp_replace('createtab_stmt', "\'$","/*/*/*.parquet'"))\
  .withColumn("createtab_stmt",concat(lit("use SAW_DB_PROD01;\n"),"createtab_stmt",lit(");")))
  meta_list.append(metadata_df.collect()[0])
  
display(meta_list)
