# Test reading the rainfields gauge netcdf files and writing the contents to a database 

from pymongo import MongoClient
import os
import datetime
# import sys
# sys.path.insert(1,"../src")
import gauge_data 

# traverse the file directories and write the data out to the database 
start_date = datetime.datetime(2021,12,7) 
end_date = datetime.datetime(2021,12,8)
time_step = datetime.timedelta(minutes=15)
base_dir = "/home/awseed/data/RF3/gaugeobs/0"
product = "gaugeobs.nc"
stn_id = 0 
start_time = datetime.datetime.now() 
db_name = 'gauge-data'
test_path = "/home/awseed/data/RF3/gaugeobs/0/2021/12/07/0_20211207_210000.gaugeobs.nc"


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
    file_path = os.path.join(base_dir,vtime.strftime("%Y/%m/%d/"))
    file_name = f"{stn_id}_{ymd}_{hms}.{product}"
    file_path = os.path.join(file_path, file_name) 
    print(f"file path = {file_path}")
    file_id = gauge_data.load_gauge_data(file_path=test_path,db_client=db_client)
    if file_id == None:
        break 

    # vtime = vtime + time_step  
    vtime = end_date  
    file_number += 1

end_time = datetime.datetime.now() 
dt = end_time - start_time 
print(f"Processed {file_number} files in {dt}")
client.close()