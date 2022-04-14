""" Test the get_gauge_data function  

    Returns:
        _type_: _description_
"""

import os
import datetime
from unittest import result
from pymongo import MongoClient 

def get_gauge_data(**kwargs):
    """Read gauge data from the MongoDB collection 
    kwargs:
        db_client: MongoDB client for the output database
        db_name: Name of the MongoDB collection for the output database
        station_id: station id for the gauge

    Returns:
        list: list of GaugeDataModels with the gauge data
    """
    # Parse the key words
    gauge_db_client = kwargs.get("db_client", None)
    gauge_db_name = kwargs.get("db_name", None)
    if gauge_db_client is None and gauge_db_name is None:
        print("Need one of db_name or db_client")
        return None
    station_id = kwargs.get("station_id", None)

    # open the connection to the database if required
    if gauge_db_name is not None:
        myclient = MongoClient()
        gauge_db_client = myclient[gauge_db_name]
    gauges = gauge_db_client["gauge_data"]

    result = []
    for gauge in gauges.find({"properties.station_id":station_id}):
        result.append(gauge)  

    return result

START_DATE = datetime.datetime(2021, 12, 7, 21, 0)
END_DATE = datetime.datetime(2021, 12, 7, 23, 45)
TIME_STEP = datetime.timedelta(minutes=15)
DB_NAME = 'gaugeobs'
STATION_ID = 1007

# drop the existing data base for the test
client = MongoClient()
db_client = client[DB_NAME]
data = get_gauge_data(db_client = db_client,station_id=1007)
client.close()
print(data)
