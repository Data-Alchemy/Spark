def df_builder_inline(type: str,file_format:str , zone:str , subject_area: str , directory: str, file_name:str = None, opts:str = None , gen_schema:bool =False) ->SparkDataFrame:
  ### csv files are the worst so we need to set header option to read the file correctly ###
  try:
      ######################################################################################
      ##################       finds  file in adls to read  from       #####################
      if file_name != None: 
        option = None
        file_name =f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}"
        schema_file = f"/dbfsmnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}"
      
      if type== "latest":
        option = None
        file_list   = recursive_ls(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}")
        latest_file = f"/dbfs{file_list[-1][5:]}"
        file_name   =  f"dbfs:{file_list[-1][5:]}"
        schema_file = f"/dbfs{file_list[-1][5:]}"
        #f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}"
        
        
      if type == "all":
        option = '"recursiveFileLookup", "true"'
        file_name = f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/"
        
        file_list   = recursive_ls(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}")
        schema_file = f"/dbfs{file_list[-1][5:]}"
      ######################################################################################
      ##############   generates parquet schema without reading data      ##################
      
      if type != 'all' and file_format == "parquet": 
        myschema = read_parquet_schema_df(path= schema_file)
        parquet_schema= PySparkParquetSchema(Schema = myschema)
      elif type == 'all' and file_format  == "parquet":   
        myschema = read_parquet_schema_df(path= schema_file)
        parquet_schema= PySparkParquetSchema(Schema = myschema)
                                                          #f"/mnt/adls_sofdl{zone}_{subject_area}/{directory}/*."+file_format)
      ######################################################################################  
      
      if file_format == "csv":
          return spark.read.format(f"{file_format}").option("header","true").load(file_name)
        
      elif file_format == "parquet" and gen_schema ==True:
            return spark.read.option("inferSchema" , "false").format(f"{file_format}").schema(parquet_schema).option(option).load(file_name)

      elif file_format == "parquet" and gen_schema ==False:
            return spark.read.option("inferSchema" , "True").format(f"{file_format}").load(file_name)
    
      elif file_format == "delta" and gen_schema ==False: 
            return spark.read.option("inferSchema" , "true").format(f"{file_format}").option(option).load(file_name)
      else:
        print("file ext not recognized for df transform")
  except Exception as e: # work on python 3.x
       print(f'File ext not recognized for df transform for directory: {directory} \n file :{file_name} \n exception message: : ', str(e))
      
