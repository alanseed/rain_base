""" Test the get_gauge_data function  

    Returns:
        _type_: _description_
"""

import os
from datetime import datetime, timedelta
from pymongo import MongoClient 

def get_gauge_data(**kwargs):
    """Read gauge data from the MongoDB collection 
    kwargs:
        db_client: MongoDB client for the output database
        db_name: Name of the MongoDB collection for the output database
        station_id: station id for the gauge
        start_time: minimum time for the data
        end_time: maximum time for the data, need both start_time and end_time arguments for a search on time

    Returns:
        MongoDB cursor object sorted on valid_time
    """
    # Parse the key words
    gauge_db_client = kwargs.get("db_client", None)
    gauge_db_name = kwargs.get("db_name", None)
    if gauge_db_client is None and gauge_db_name is None:
        print("Need one of db_name or db_client")
        return None
    station_id = kwargs.get("station_id", None)
    start_time = kwargs.get("start_time", None)
    end_time = kwargs.get("end_time", None)

    # open the connection to the database if required
    if gauge_db_name is not None:
        myclient = MongoClient()
        gauge_db_client = myclient[gauge_db_name]
    gauges = gauge_db_client["gauge_data"]

    query = {}
    if station_id is not None:
        query['properties.station_id']=station_id
    if (start_time is not None) and (end_time is not None):
        query['properties.valid_time'] = {'$gte':int(datetime.timestamp(start_time)),'$lte':int(datetime.timestamp(end_time))} 
    cursor = gauges.find(query).sort('properties.valid_time')
    return cursor

START_DATE = datetime(2021, 12, 8, 8, 0)
END_DATE = datetime(2021, 12, 8, 23, 45)
TIME_STEP = timedelta(minutes=15)
DB_NAME = 'gaugeobs'
STATION_ID = 597506

# drop the existing data base for the test
client = MongoClient()
db_client = client[DB_NAME]
results = get_gauge_data(db_client = db_client,station_id=STATION_ID, start_time = START_DATE, end_time = END_DATE)
client.close()

for record in results:
    print(record)
