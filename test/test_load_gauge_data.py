""" Test the load_gauge_data function  

    Returns:
        _type_: _description_
"""
import os
import datetime
from pymongo import MongoClient, ASCENDING

from package import gauge_data

START_DATE = datetime.datetime(2021, 12, 7, 21, 0,tzinfo=datetime.timezone.utc)
END_DATE = datetime.datetime(2021, 12, 8, 0, 0,tzinfo=datetime.timezone.utc)
TIME_STEP = datetime.timedelta(minutes=15)
BASE_DIR = "/home/awseed/data/RF3/gaugeobs/0"
PRODUCT = "gaugeobs.nc"
STN_ID = 0
DB_NAME = 'gaugeobs'
TEST_PATH = "/home/awseed/data/RF3/gaugeobs/0/2021/12/07/0_20211207_210000.gaugeobs.nc"

# drop the existing data base for the test
client = MongoClient()
dbnames = client.list_database_names()
if DB_NAME in dbnames:
    client.drop_database(DB_NAME)

db_client = client[DB_NAME]

vtime = START_DATE
file_number = 0
start_time = datetime.datetime.now()
while vtime <= END_DATE:
    y = vtime.strftime("%Y")
    ymd = vtime.strftime("%Y%m%d")
    hms = vtime.strftime("%H%M%S")
    file_path = os.path.join(BASE_DIR, vtime.strftime("%Y/%m/%d/"))
    file_name = f"{STN_ID}_{ymd}_{hms}.{PRODUCT}"
    file_path = os.path.join(file_path, file_name)
    file_id = gauge_data.load_gauge_data(file_path=file_path, db_client=db_client)
    if file_id is None:
        break

    vtime = vtime + TIME_STEP
    file_number += 1

end_time = datetime.datetime.now()
dt = end_time - start_time
print(f"Processed {file_number} files in {dt}")

# set the 2d location and valid_time as indexes 
resp = db_client["gauge_data"].create_index([('properties.valid_time',ASCENDING)],name='ValidTimeIndex')
print(resp)
resp = db_client["gauge_data"].create_index([('geometry','2dsphere')], name='LocationIndex')
print(resp)

client.close()
