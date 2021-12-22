
from pyspark.sql import DataFrame as SparkDataFrame
def df_builder_inline(type: str,file_format:str , zone:str , subject_area: str , directory: str, file_name:str = None, opts:str = None , gen_schema:bool =False) ->SparkDataFrame:
  ### csv files are the worst so we need to set header option to read the file correctly ###
  try:
      ######################################################################################
      ##################    finds latest file in adls to read  from    #####################
      if type== "latest":
        file_list = recursive_ls(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}")
        latest_file = f"/dbfs{file_list[-1][5:]}"

      
      ######################################################################################
      ##############   generates parquet schema without reading data      ##################
      
      if type != 'all' and file_format == "parquet": 
        myschema = read_parquet_schema_df(path= f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        parquet_schema= PySparkParquetSchema(Schema = myschema)
      elif type == 'all' and file_format == file_format == "parquet":   
        schema_files = recursive_ls(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}")
        latest_schema_file = f"/dbfs{schema_files[-1][5:]}"
        myschema = read_parquet_schema_df(path= latest_schema_file)
        parquet_schema= PySparkParquetSchema(Schema = myschema)
                                                          #f"/mnt/adls_sofdl{zone}_{subject_area}/{directory}/*."+file_format)
      ######################################################################################  
      
      if file_format == "csv":
        if file_name != None:  
          return spark.read.format(f"{file_format}").option("header","true").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'daily':
          return  spark.read.format(f"{file_format}").option("header","true").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'all':
            return spark.read.format(f"{file_format}").option("recursiveFileLookup", "true").option("header","true").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}")

      elif file_format == "parquet" and gen_schema ==True:
        if file_name != None:  
            return spark.read.option("inferSchema" , "false").format(f"{file_format}").schema(parquet_schema).load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
          #spark.read.format(f"{file_format}").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'daily':
            return  spark.read.option("inferSchema" , "false").format(f"{file_format}").schema(parquet_schema).load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'all':
            return spark.read.option("inferSchema" , "false").format(f"{file_format}").schema(parquet_schema).option("recursiveFileLookup", "true").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/")

      elif file_format == "parquet" and gen_schema ==False:
        if file_name != None:  
            return spark.read.option("inferSchema" , "True").format(f"{file_format}").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
          #spark.read.format(f"{file_format}").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'daily':
            return  spark.read.option("inferSchema" , "True").format(f"{file_format}").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'all':
            return spark.read.format(f"{file_format}").option("recursiveFileLookup", "true").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}")
          
      elif file_format == "delta" and gen_schema ==False:
        if file_name != None:  
            return spark.read.option("inferSchema" , "true").format(f"{file_format}").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
          #spark.read.format(f"{file_format}").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'daily':
            return  spark.read.option("inferSchema" , "true").format(f"{file_format}").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'all':
            return spark.read.option("inferSchema" , "true").format(f"{file_format}").option("recursiveFileLookup", "true").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/")

      else:
        print("file ext not recognized for df transform")
  except Exception as e: # work on python 3.x
       print(f'File ext not recognized for df transform for {directory}{file_name}: ', str(e))
      
    
'''sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")'''

