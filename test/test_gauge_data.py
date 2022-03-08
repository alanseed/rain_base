# Test reading the rainfields gauge netcdf files and writing the contents to a database 

from pymongo import MongoClient
import os
import datetime 
import netCDF4 as nc  
from geojson import Point 

# Write the data to the MongoDB
# returns None on error 
def load_gauge_data(**kwargs): 
    rf3_name = kwargs.get("file_path",None)
    db = kwargs.get("db",None)
    db_name = kwargs.get("db_name", None) 
    if db == None and db_name == None:
        print("Need one of db_name or db_client")
        return None
    if rf3_name ==  None: 
        print(f"file_path keyword not found")
        return None

    # open the ncfile 
    try:
        ds = nc.Dataset(rf3_name)        
    except FileNotFoundError : 
        print(f"Error: {rf3_name} not found")
        return None

    # read in the variables 
    number_stations = ds.dimensions['stations'].size 
    print(f"found {number_stations} stations") 

    start_time = ds['start_time'][0].item()
    valid_time = ds['valid_time'][0].item()
    
    station_id = ds['station_id'][:]
    latitude = ds['latitude'][:]
    longitude = ds['longitude'][:]
    precipitation = ds['precipitation'][:]
    latest_obs_time = ds['latest_obs_time'][:]

    # loop over the stations and load up the gauge_list 
    gauge_list = []
    for ia in range (1,number_stations): 
        lat = latitude[ia].item()
        lon = longitude[ia].item() 
        loc = Point([lon,lat])
        record = {
            'start_time':start_time,
            'valid_time':valid_time,
            'station_id':station_id[ia].item(),
            'precipitation':precipitation[ia].item(),
            'latest_obs_time':latest_obs_time[ia].item(),
            'location':loc
        }
        gauge_list.append(record)

    # write to the database 
    if db_name != None: 
        myclient = MongoClient()
        db = myclient[db_name]
    gauge_collection = db["gauge_data"] 
    gauge_collection.insert_many(gauge_list)

    if db_name != None: 
        myclient.close()

    print(f"written {rf3_name}")
    return 0


# import gauge_data 

# traverse the file directories and write the data out to the database 
start_date = datetime.datetime(2021,12,7) 
end_date = datetime.datetime(2021,12,8)
time_step = datetime.timedelta(minutes=15)
base_dir = "/home/awseed/data/RF3/gaugeobs/0"
product = "gaugeobs.nc"
stn_id = 0 
start_time = datetime.datetime.now() 
db_name = 'gaugeobs'
test_path = "/home/awseed/data/RF3/gaugeobs/0/2021/12/07/0_20211207_210000.gaugeobs.nc"


# drop the existing data base for the test
client = MongoClient() 
dbnames = client.list_database_names() 
if db_name in dbnames: 
    client.drop_database(db_name)

db = client[db_name]
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
    file_id = load_gauge_data(file_path=test_path,db=db)
    if file_id == None:
        break 

    # vtime = vtime + time_step  
    vtime = end_date  
    file_number += 1

end_time = datetime.datetime.now() 
dt = end_time - start_time 
print(f"Processed {file_number} files in {dt}")
client.close()