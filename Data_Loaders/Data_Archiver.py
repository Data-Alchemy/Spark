### Archive after loading files in ###
import os
import re
import datetime as dt

###################################
source = '' # eg : /InTransit/ReceivingRepository/
target = '' #eg : /InTransit/ToIngest
mnt = '' # eg : dbfs:/mnt/adls_deloitte_dl
source_path= mnt+source
target_path= mnt+target
###################################

today = dt.datetime.now().date()

file_list = [file for file in os.listdir(path)] #if dt.datetime.fromtimestamp(os.path.getctime(path+ '/' + file)) ==today])
#file_list = file_list[0:2]
i = 0
for file in file_list:
  i+=1
  
  parent_folder = re.sub("(\d)|(-.*)","", file)
  #print(parent_folder)
  if "_Y" in file and "_Q" in file:
    Load_Year = re.findall('\d+|$', file)[0]
    Load_Quarter = re.findall('\d+|$', file)[1]
    Load_Month   = None
    target_path = f'{target_path}/{parent_folder}/{Load_Year}/Q{Load_Quarter}/{file}'

  elif "_Y" in file and "_Q" not in file:
    Load_Year    = int(re.findall('\d+|$', file)[0])
    Load_Quarter = None
    Load_Month   = None
    Load_filter  = f"where Load_Year = {Load_Year}"
    child_folder_l1 = re.findall('\d+|$', file)[0]
    target_path = f'{target_path}/{parent_folder}/{child_folder_l1}/{file}'

    
  else:
    parent_folder = re.sub("(\d)|(-.*)","", file)
    child_folder_l1 =re.sub("[^\d]","", file)[-8:-4]
    child_folder_l2 =re.sub("[^\d]","", file)[-4:-2]
    target_path = f'{target_path}/{parent_folder}/{child_folder_l1}/{child_folder_l2}/{file}'

    
  source_path = f"{source_path}/{file}"

  print("source:",source_path,'\n',"target:",target_path)
  dbutils.fs.mv(source_path,target_path, recurse = True)
print('files read:',i)
