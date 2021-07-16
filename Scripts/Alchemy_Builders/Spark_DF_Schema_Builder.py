import pyarrow.parquet as pq
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import DataFrame as SparkDataFrame

def conv_to_structtype_dtype(dtype):
    if dtype == 'datetime64[ns]'     : return TimestampType()
    elif dtype == 'date32[day]'      : return IntegerType()
    elif dtype =="time64[us]"        : return TimestampType()
    elif dtype == 'int64'            : return LongType()
    elif dtype == 'int32'            : return IntegerType()
    elif dtype == 'float64'          : return FloatType()
    elif dtype == 'bool'             : return BooleanType()
    elif dtype == 'binary'           : return BinaryType()
    else: return StringType()
    
def build_struct(col,dtype):
  try:
    data_type = conv_to_structtype_dtype(dtype)
  except:
    data_type = StringType()
  return StructField(col,data_type)

def read_parquet_schema_df(path: str, column_name_length: int = 50 ) -> dict:#pd.DataFrame:
    illegal_dtypes = ["list<item: null>","null","list<item: string>"] # list of data types not supported in spark df
    #Return a dictionary corresponding to the schema of a local path of a parquet file.#
    schema = pq.read_schema(path, memory_map=True)
    schema = pd.DataFrame(({"column": name, "dtype": str(pa_dtype)} for name, pa_dtype in zip(schema.names, schema.types)))
    schema = schema.reindex(columns=["column", "dtype"], fill_value=pd.NA)  # Ensures columns in case the parquet file has an empty dataframe.
    schema['trunc_col_name']= schema['column'].apply(lambda x: x[:column_name_length] if len(x) > column_name_length else x) # for columns names that are too long cuts them to specifified length
    schema['dtype']= schema['dtype'].apply(lambda col: "string" if col in illegal_dtypes else col)
    schema['dict_dtypes']= list(zip(schema["column"],schema["dtype"]))
    #schema['dict_dtypes']= schema.apply(lambda x: x["column"]+':'+x["dtype"], axis = 1)
    schema['dict_dtypes_trnc_column_name']= schema.apply(lambda x: f''''{x["trunc_col_name"]}':{x["dtype"]}''', axis = 1)
    schema.reset_index(drop = True,inplace = True)
    schema['SUID'] = range(1, 1+len(schema))
    schema.set_index("SUID")
    schema = schema[["SUID","dict_dtypes","dtype","column"]]
    return zip(schema.column,schema.dtype)#schema#

def PySparkParquetSchema(Schema):
  struct = []
  for c,t in Schema:
    struct.append(build_struct(c,t))
  parquet_schema = StructType(struct)
  return parquet_schema

def recursive_ls(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

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
        elif type == 'daily':
            return  spark.read.option("inferSchema" , "true").format(f"{file_format}").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/{file_name}")
        elif type == 'all':
            return spark.read.option("inferSchema" , "true").format(f"{file_format}").option("recursiveFileLookup", "true").load(f"dbfs:/mnt/adls_sofdl{zone}_{subject_area}/{directory}/")

      else:
        print("file ext not recognized for df transform")
  except Exception as e: # work on python 3.x
       print(f'File ext not recognized for df transform for {directory}{file_name}: ', str(e))
