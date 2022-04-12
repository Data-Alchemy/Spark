import cx_Oracle,pandas as pd,datetime
import pyodbc as odbc
import ssas_api
from rich_dataframe import prettify
import rich


########################## Pandas Settings ################################
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


###########################################################################


##### Parms / Variables #####
variance_days = 7
delta_days= 1


current_year = datetime.datetime.strftime(datetime.datetime.now() -
    datetime.timedelta(days=delta_days, hours=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%H')), minutes=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%M')), seconds=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%S'))), '%Y')#/%m/%d %H:%M:%S %p')

current_day = datetime.datetime.strftime(datetime.datetime.now() -
    datetime.timedelta(days=delta_days, hours=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%H')), minutes=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%M')), seconds=int(
    datetime.datetime.strftime(datetime.datetime.now(), '%S'))), '%Y-%m-%d')



################################
##### Oracle GL Connection #####
################################

ora_dsn_tns = cx_Oracle.makedsn('', '1521', service_name='') # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
ora_conn    = cx_Oracle.connect(user=r'', password='', dsn=ora_dsn_tns) # if needed, place an 'r' before any parameter in order to address special characters such as '\'. For example, if your user name contains '\', you'll need to place 'r' before the user name: user=r'User Name'


##########################################
##### Synapse Retail Sale Connection #####
##########################################
server    = ''
database  = ''
username  = ''
password  =''
driver    = '{ODBC Driver 17 for SQL Server}'
conn_synapse = odbc.connect(f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password};Authentication=ActiveDirectoryPassword;Encrypt=yes;')
##########################################
#####   AAS Retail Sale Connection   #####
##########################################

dbname          = 'Model'
process_options = {'all':'item.id','single':dbname}
usr             = "''" #must keep quotes
pwd             = f"'{open('C:/Users/545001/Python_Automation/disKys.txt').readline()}'"
process_type    = process_options['single']
aas_server_list = {
                    'dev' :'',
                    'qa'  :'',
                    'prod':''
                  }
conn_aas = ssas_api.set_conn_string(server=aas_server_list['prod'],db_name= dbname,username= usr,password=pwd)

################################
################################

################ Queries ###################
df_ora = pd.read_sql(f'''
select 
*
from table
 order by field asc
''',
                     con=ora_conn)

##  ##
df_synapse = pd.read_sql(f'''
select 
*
from table
 order by field asc
''', con=conn_synapse)

##  ##
dax_string = f'''
//any valid DAX query
  EVALUATE
    FILTER(
      SUMMARIZECOLUMNS(
        'Dim Date'[Date],
        "Financial Sales",
        [Daily Financial Sales CY]
        ),
      'Dim Date'[Date] >= TODAY() - {variance_days} 
      && 'Dim Date'[Date] <= TODAY() - 1
      )
'''
df_aas = ssas_api.get_DAX(connection_string=conn_aas, dax_string=dax_string)
df_aas.rename(columns={'Dim Date[Date]':'Date','[Financial Sales]':'Financial Sales'},inplace=True)
df_aas['Date'] =pd.to_datetime(df_aas['Date'])
#print(df_aas)


###########################################
########## Compare operations #############

df_synapse["Date"]                        = pd.to_datetime(df_synapse['Date'])
df_compare                                = pd.merge(df_ora,df_synapse,how='inner',on=["Date"]).merge(df_aas,how='inner',on=["Date"])
df_compare["Retail_Sales"]                = df_compare["Retail_Sales"].astype('int64')
df_compare['Sales Variance AAS']          = round((df_compare["Metric"].sub(df_compare["Financial Sales"],axis = 0)),2)
df_compare['Sales Variance Percent AAS']  = (df_compare["GL_Sales"].sub(df_compare["Financial Sales"],axis = 0)/df_compare["GL_Sales"])
df_compare['Transaction_Count_Alert']     = df_compare['Sales Variance Percent AAS'].apply(lambda x: 'Error' if x >=1 else 'OK' )
df_compare['Transaction_Amount_Alert']    = df_compare['Sales Variance Percent AAS'].apply(lambda x: 'Error' if x >=1 else 'OK' )

for col in ['GL_Sales', 'Retail_Sales', 'Financial Sales','Sales Variance AAS']:
    df_compare[col] = df_compare[col].apply(lambda x: f"${x:,.2f}")

### style transforms ###

#print(prettify(df=df_compare))
df_compare= df_compare[["YEAR","MONTH","Date","GL_Sales","Retail_Sales","Financial Sales","Sales Variance AAS","Sales Variance Percent AAS","Transaction_Amount_Alert"]]
############################################
####### send alert if error found  #########


filter1 =df_compare["Transaction_Amount_Alert"] == "Error"
print(df_compare)
alert_df = df_compare[filter1]
print(alert_df[["YEAR","MONTH","Date","Sales Variance AAS",'Sales Variance Percent AAS','Transaction_Amount_Alert']])

