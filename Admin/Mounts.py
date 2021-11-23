try:

  clientKey = dbutils.secrets.get("Production_Datalake","ClientKey")
except Exception as e:
  print('Unable to obtain secret from keyvault exception is:',e)
  
clientId  = ''
tenantId = ""
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientId,
           "fs.azure.account.oauth2.client.secret": clientKey,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + tenantId + "/oauth2/token"}

ListOfAccountsPrd =["","",""]
ListofSubjectAreas =['master-data', "retail-data","pharmacy-data","people-data","supply-chain-data"]
for acnt in ListOfAccountsPrd:
  for sub in ListofSubjectAreas:
    print(f"/mnt/adls_{acnt}_{sub}")
    #dbutils.fs.unmount(mount_point = f"/mnt/adls_{acnt}_{sub}")
    dbutils.fs.mount(source = "abfss://" + sub + "@" + acnt + ".dfs.core.windows.net/",mount_point = f"/mnt/adls_{acnt}_{sub}",extra_configs = configs)
    
