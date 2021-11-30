import os
import re


###################################
source  = ''
target  = ''
mnt     = ''
path    = mnt+source
###################################

file_list = [file for file in os.listdir(path)] #if dt.datetime.fromtimestamp(os.path.getctime(path+ '/' + file)) ==today])
for file in file_list:
  parent_folder = re.sub("(\d)|(-.*)","", file)

  if "_Y" in file and "_Q" in file:
    child_folder_l1 = re.findall('\d+|$', file)[0]
    child_folder_l2 = re.findall('\d+|$', file)[1]
    path = f'/mnt/adls_deloitte_dl/ReceivingRepositoryArchive/{parent_folder}/{child_folder_l1}/Q{child_folder_l2}/{file}'

  elif "_Y" in file and "_Q" not in file:
    child_folder_l1 = re.findall('\d+|$', file)[0]
    path = f'/mnt/adls_deloitte_dl/ReceivingRepositoryArchive/{parent_folder}/{child_folder_l1}/{file}'

  else:
    parent_folder = re.sub("(\d)|(-.*)","", file)
    child_folder_l1 =re.sub("[^\d]","", file)[-8:-4]
    child_folder_l2 =re.sub("[^\d]","", file)[-4:-2]
    path = f'/mnt/adls_deloitte_dl/ReceivingRepositoryArchive/{parent_folder}/{child_folder_l1}/{child_folder_l2}/{file}'
  source_path = f"/mnt/adls_deloitte_dl/InTransit/ReceivingRepository/{file}"
  target_path = path
  print("source:",source_path,'\n',"target:",target_path)
  dbutils.fs.mv(source_path,target_path)
