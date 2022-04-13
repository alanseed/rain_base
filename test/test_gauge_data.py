""" Test reading a set of rainfields3 gauge obs files into the database

    Returns:
        _type_: _description_
"""


import os
import datetime
from netCDF4 import Dataset
from pymongo import MongoClient
from geojson import Point

START_DATE = datetime.datetime(2021, 12, 7, 21, 0)
END_DATE = datetime.datetime(2021, 12, 7, 23, 45)
TIME_STEP = datetime.timedelta(minutes=15)
BASE_DIR = "/home/awseed/data/RF3/gaugeobs/0"
PRODUCT = "gaugeobs.nc"
STN_ID = 0
DB_NAME = 'gaugeobs'
TEST_PATH = "/home/awseed/data/RF3/gaugeobs/0/2021/12/07/0_20211207_210000.gaugeobs.nc"

# Write the data to the MongoDB
# returns None on error


def load_gauge_data(**kwargs):
    """Read a rainfields3 rain gauge netcdf file and write the data to the database 

    Returns:
        None: on error 
        0: on success 
    """
    rf3_name = kwargs.get("file_path", None)
    gauge_db_client = kwargs.get("db_client", None)
    gauge_db_name = kwargs.get("db_name", None)
    if gauge_db_client is None and gauge_db_name is None:
        print("Need one of db_name or db_client")
        return None
    if rf3_name is None:
        print("file_path keyword not found")
        return None

    # open the ncfile
    try:
        rf3_ds = Dataset(rf3_name)
    except FileNotFoundError:
        print(f"Error: {rf3_name} not found")
        return None

    # read in the variables
    number_stations = rf3_ds.dimensions['stations'].size
    print(f"found {number_stations} stations")
    rf3_valid_time = rf3_ds['valid_time'][0].item()

    station_id = rf3_ds['station_id'][:]
    latitude = rf3_ds['latitude'][:]
    longitude = rf3_ds['longitude'][:]
    precipitation = rf3_ds['precipitation'][:]
    latest_obs_time = rf3_ds['latest_obs_time'][:]

    # loop over the stations and load up the gauge_list
    gauge_list = []
    for ia in range(1, number_stations):
        lat = latitude[ia].item()
        lon = longitude[ia].item()
        loc = Point([lon, lat])
        record = {
            'start_time': start_time,
            'valid_time': rf3_valid_time,
            'station_id': station_id[ia].item(),
            'precipitation': precipitation[ia].item(),
            'latest_obs_time': latest_obs_time[ia].item(),
            'location': loc
        }
        gauge_list.append(record)

    # write to the database
    if gauge_db_name is not None:
        myclient = MongoClient()
        gauge_db_client = myclient[gauge_db_name]
    gauge_collection = gauge_db_client["gauge_data"]
    gauge_collection.insert_many(gauge_list)

    if gauge_db_name is not None:
        myclient.close()

    print(f"written {rf3_name}")
    return 0


# drop the existing data base for the test
client = MongoClient()
dbnames = client.list_database_names()
if DB_NAME in dbnames:
    client.drop_database(DB_NAME)

db_client = client[DB_NAME]
vtime = START_DATE
file_number = 0
start_time = datetime.datetime.now()
while vtime < END_DATE:
    y = vtime.strftime("%Y")
    ymd = vtime.strftime("%Y%m%d")
    hms = vtime.strftime("%H%M%S")
    file_path = os.path.join(BASE_DIR, vtime.strftime("%Y/%m/%d/"))
    file_name = f"{STN_ID}_{ymd}_{hms}.{PRODUCT}"
    file_path = os.path.join(file_path, file_name)
    print(f"file path = {file_path}")
    file_id = load_gauge_data(file_path=file_path, db_client=db_client)
    if file_id is None:
        break

    vtime = vtime + TIME_STEP
    file_number += 1

end_time = datetime.datetime.now()
dt = end_time - start_time
print(f"Processed {file_number} files in {dt}")
client.close()
