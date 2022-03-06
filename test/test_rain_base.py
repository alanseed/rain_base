from pymongo import MongoClient
import os
import datetime
import sys
sys.path.insert(1,"../src")
import rain_base_fs 

# traverse the file directories and write the data out to the database 
start_date = datetime.datetime(2021,1,28) 
end_date = datetime.datetime(2021,1,29)
time_step = datetime.timedelta(minutes=5)
base_dir = "/home/awseed/data/RF3/prcp-crate"
product = "prcp-crate.nc"
stn_id = 310 
start_time = datetime.datetime.now() 
db_name = 'grid-data'

# drop the existing data base 
client = MongoClient() 
dbnames = client.list_database_names() 
if db_name in dbnames: 
    client.drop_database(db_name)

db_client = client[db_name]
file_number = 0 
vtime = start_date
while vtime < end_date:
    y = vtime.strftime("%Y") 
    ymd = vtime.strftime("%Y%m%d")
    hms = vtime.strftime("%H%M%S")
    file_path= os.path.join(base_dir,y)
    file_path = os.path.join(file_path,ymd)
    file_name = f"{stn_id}_{ymd}_{hms}.{product}"
    file_path = os.path.join(file_path, file_name) 
    file_id = rain_base_fs.write_to_rain_basefs(file_path=file_path,db_client=db_client)
    if file_id == None:
        break 

    vtime = vtime + time_step  
    file_number += 1

end_time = datetime.datetime.now() 
dt = end_time - start_time 
print(f"Processed {file_number} files in {dt}")
client.close()