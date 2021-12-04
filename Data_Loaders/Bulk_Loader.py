import os
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

################################################################
#################  parameters for loading job  #################

source = f''
target = '/Tables/'
mnt = ''
path= mnt+source
dbfs_path = f'dbfs:/mnt/{mnt}/{source}/'
target_path = f'dbfs:/mnt/{mnt}/{target}'
################################################################
################################################################
window = Window.orderBy(col('m_increasing_id'))

file_list = [file for file in os.listdir(path)]

for file in file_list:
  parent_folder = re.sub("(\d)|(-.*)","", file)
  file_year     = re.sub("[^\d]","", file)[-8:-4]
  file_month    = re.sub("[^\d]","", file)[-4:-2]
  file_day      = re.sub("[^\d]","", file)[-2:]
  
  # Determines Loading Logic #
  if "_Y" in file and "_Q" in file:
        Load_Year    = int(re.findall('\d+|$', file)[0])
        Load_Quarter = "Q"+re.findall('\d+|$', file)[1]
        Load_Month   = None
        Load_filter  = f"where Load_Year = {Load_Year} and Load_Quarter = {Load_Quarter}"
        
  elif "_Y" in file and "_Q" not in file:
        Load_Year    = int(re.findall('\d+|$', file)[0])
        Load_Quarter = None
        Load_Month   = None
        Load_filter  = f"where Load_Year = {Load_Year}"
        
  else:
        Load_Year        = re.sub("[^\d]","", file)[-8:-4]
        Load_Quarter     = None
        Load_Month       = re.sub("[^\d]","", file)[-4:-2]
        Load_filter      = f"where Load_Year = {Load_Year}"
###############################
## read the file into memory ##  

  existing_table_df = (spark.read.format('csv')
               .option('header', 'True')
               .option("inferSchema", "true")
               .load(f'{dbfs_path}{file}'))

## if dumb characters are added to column names replace with _ ##
  existing_table_df =existing_table_df.toDF(*(re.sub("[^0-9a-zA-Z$]+","_",c) for c in existing_table_df.columns))

###############################
## add metadata columns 2 df ## 

  existing_table_df= existing_table_df.withColumn("md5",md5(concat_ws("",*existing_table_df.columns)))
  existing_table_df = existing_table_df.dropDuplicates(["md5"])

  existing_table_df = existing_table_df                     \
  .withColumn("Load_Year"     , lit(int(f"{Load_Year}")))   \
  .withColumn("Load_Quarter"  , lit(f"{Load_Quarter}"))     \
  .withColumn("Load_Month"    , lit(f"{Load_Month}"))       \
  .withColumn("File_Name"     , lit(f"{file}"))             \
  .withColumn("File_Year"     , lit(int(f"{file_year}")))   \
  .withColumn("File_Month"    , lit(int(f"{file_month}")))  \
  .withColumn("File_Day"      , lit(int(f"{file_day}")))    \
  .withColumn("Processed_Date", lit(processing_date))       \
  .withColumn("Processed_By"  , lit("DIS"))

###############################
## Create TMP Table to Merge ## 
  tblList        = sqlContext.tableNames("Flash_Sales")
  table_location = f'{target_path}{parent_folder}'

  ## when table already exists in metastore merge dataframe to add new data ##
  
  if parent_folder.replace(" ","").upper() in [str(t).replace(" ","").upper() for t in tblList]:
    
      print(f"Table exists inserting records to existing table Flash_Sales.{parent_folder} using csv file {file}")
      existing_table_df.createOrReplaceTempView(f"Load_{parent_folder}")
      
      query = f'''
        MERGE INTO Flash_Sales.{parent_folder} USING
        (
        Select * from Load_{parent_folder}
        ) Merge_from
        on 
        (
        Merge_from.md5 = Flash_Sales.{parent_folder}.md5
        )
        when not matched then INSERT *
      '''
      
      spark.sql(f"{query}")
      ##########################################
      ## delete delta history as its not used ##
      spark.sql('''SET spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled = false''')
      spark.sql('''SET spark.databricks.delta.retentionDurationCheck.enabled = false''')
      spark.sql(f'''VACUUM delta.`{table_location}`RETAIN 0 HOURS''')
      ##########################################

  ## when new file not in metastore is added create new table dynamically ##
  else:

      new_table_df = existing_table_df
      query = f'''CREATE TABLE Flash_Sales.{parent_folder} USING DELTA LOCATION '{table_location}' '''
      print(f"Table does not exist, creating new table {parent_folder} using csv file {file}")

      try:

        new_table_df.write.partitionBy("File_Year","File_Month").format("delta").save(table_location) 
        spark.sql(f"{query}")

      except Exception as e:
        print("exception is :",e)
