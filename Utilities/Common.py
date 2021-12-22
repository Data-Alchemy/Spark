import pyarrow.parquet as pq
import pandas as pd
from pyspark.sql.types import *

def conv_to_structtype_dtype(dtype):
    if dtype == 'datetime64[ns]'     : return TimestampType()
    elif dtype == 'date32[day]'      : return IntegerType()
    elif dtype =="time64[us]"       : return TimestampType()
    elif dtype == 'int64'            : return LongType()
    elif dtype == 'int32'            : return IntegerType()
    elif dtype == 'float64'          : return FloatType()
    elif dtype == 'bool'             : return BooleanType()
    else: return StringType()
    
def build_struct(col,dtype):
  try:
    data_type = conv_to_structtype_dtype(dtype)
  except:
    data_type = StringType()
  return StructField(col,data_type)

def spark_shape(self):
    return {'rows':'{:,}'.format(self.count()), 'columns':len(self.columns)}
pyspark.sql.dataframe.DataFrame.shape = spark_shape

def read_parquet_schema_df(path: str, column_name_length: int = 50 ) -> dict:#pd.DataFrame:
    illegal_dtypes = ["list<item: null>","null","list<item: string>"] # list of data types not supported in spark df
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

def file_exists(current_file:str , subject_area: str, zone:str , directory:str ) ->bool:
  listoffiles = [file.name for file in dbutils.fs.ls(f"dbfs:/mnt/adls_dl{zone}_{subject_area}/{directory}")]
  if current_file in listoffiles: return True
  else: return false 

def PySparkParquetSchema(Schema):
  struct = []
  for c,t in Schema:
    struct.append(build_struct(c,t))
  parquet_schema = StructType(struct)
  return parquet_schema

def recursive_ls(ls_path): #version 2 #
  dir_paths= dbutils.fs.ls(ls_path)
  subdir_paths =[p.path for p in dir_paths if p.isDir() and 'UNPROCESSED' not in p.path and int(p.size) !=0]
  if len(subdir_paths) <1 :
    flat_subdir_paths = [p.path for p in dir_paths if p.isDir()==False and int(p.size) !=0]
  else: 
    flat_subdir_paths = [p for subdir in subdir_paths for p in subdir if int(p.size) !=0]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

date = spark.sql(""" 
Select 
date_format(now()+ INTERVAL -7 hours,'yyyy') as Year,
date_format(now()+ INTERVAL -7 hours,'MM') as Month,
date_format(now()+ INTERVAL -7 hours,'yyyy-MM-dd HH:mm:ss') as Date,
date_format(now()+ INTERVAL -7 hours,'yyyy/MM') as PathDate,
date_format(date_add(now()+ INTERVAL -7 hours,-2),'yyyyMMdd')||'_'||date_format(date_add(now()+ INTERVAL -7 hours,-1),'yyyyMMdd') as date_range,
date_format(now()+ INTERVAL -7 hours,'HH:mm') as time
""").collect()[0]

processing_date = date["Date"]
processing_time = date["time"]
daily_file_path= date["PathDate"]
daily_range = date["date_range"]
daily_year = date["Year"]
daily_month =date["Month"]

