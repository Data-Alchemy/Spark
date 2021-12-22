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

def recursive_ls(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

