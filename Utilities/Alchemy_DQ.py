import os
import re
import pandas as PandasDataFrame 
import numpy as np
import chardet
import pyspark
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from datetime import date, timedelta , datetime

class Alchemy_DQ():
  
  def __init__(self,df):
    self.df     =    df
  
  def Validate_Input(self,df):
    if isinstance(df,RDD):
      self.Object = 'RDD'
      raise Exception( "This is an RDD expecting Spark Dataframe or Pandas DF")
      exit(-1)
                      
    elif isinstance(df,SparkDataFrame):
      self.Object = 'SDF'
      pass
                      
    elif isinstance(df,PandasDataFrame):
      self.Object = 'PDF'
      pass
    
    else:
      raise Exception( "This is an unknown object expecting Spark Dataframe or Pandas DF")
                      
  
  def collect_statistics(self, listofexcludedcolumns:list = None, analyzecolumn:str = None):
                      
      self.listofexcludedcolumns     = listofexcludedcolumns
      self.analyzecolumn             = analyzecolumn
                      
      if self.listofexcludedcolumns  !=None and isinstance(self.listofexcludedcolumns,list):        
        self.subset                  = [c for c in self.df.columns if c not in self.listofexcludedcolumns]
                      
      else:          
        self.subset                  = [c for c in self.df.columns]
                 
      self.df.select(self.subset).describe(self.analyzecolumn)
      

  def collect_datatypes(self,df)->list:
    self.Validate_Input(df)
    return df.dtypes
    
  def compare_dtypes(self,df2)->str:
    
    #this uses local api not spark may not work in AWS GLUE
    # using map() + reduce() to check if lists are equal
    
    self.Validate_Input(self.df)
    self.Validate_Input(df2)
    
    df1_datatypes = self.collect_datatypes(self.df)
    df2_datatypes = self.collect_datatypes(df2)
    
    if functools.reduce(lambda i, j : i and j, map(lambda m, k: m == k, df1_datatypes, df2_datatypes), True) : 
        print ("The lists are identical")
    else :
        print ("The lists are not identical")
        
  def data_completeness(self):
    #shows how many null values are present in the data 
    if self.df.count() == 0:
        raise Exception("This DataFrame contains no data")
    return self.df.select([count(when(isnan(c), c)).alias(c) for c in df.columns]).show()
    
  def infer_encoding(self,path:str):
          #this uses local api not spark may not work in AWS GLUE
          with open(path, 'rb') as rawdata:
              result = chardet.detect(rawdata.read(100000))
              print(result)
              infer_encoding  = result["encoding"]
              close()
    
