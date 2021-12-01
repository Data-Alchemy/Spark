import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

################################################################
#################  parameters for loading job  #################

source = f''
target = ''
mnt = ''
path= mnt+source
dbfs_path = f'dbfs:/mnt/{mnt}/{source}'
################################################################
################################################################


file_list = [file for file in os.listdir(path)]

for file in file_list:
  parent_folder = re.sub("(\d)|(-.*)","", file)
  print("reading file :",file)
print(parent_folder)

################################################################
existing_table_df = (spark.read.format('csv')
             .option('header', 'True')
             .option("inferSchema", "true")
             .load(dbfs_path))

################################################################
#################     Add metadata columns     #################
existing_table_df= existing_table_df\
.withColumn("File_Name",lit(f"{file}"))\
.withColumn("md5",md5(concat_ws("",*existing_table_df.columns)))

existing_table_df = existing_table_df\
.withColumn("Surrogate_Id",monotonically_increasing_id())\
.withColumn("Processed_Date",lit(processing_date))\
.withColumn("Processed_By",lit("DIS"))\


################################################################
################# removed duplicates using md5 #################

existing_table_df = existing_table_df.dropDuplicates(["md5"])
existing_table_df.createOrReplaceTempView(f"{parent_folder}")
display(existing_table_df)
