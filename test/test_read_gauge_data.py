""" Test the get_gauge_data function  

    Returns:
        _type_: _description_
"""
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from package import gauge_data

START_DATE = datetime(2021, 12, 7, 21, 0,tzinfo=timezone.utc)
END_DATE = datetime(2021, 12, 8, 0, 0,tzinfo=timezone.utc)

TIME_STEP = timedelta(minutes=15)
DB_NAME = 'gaugeobs'
STATION_ID = 597506

# open the connection to the database 
client = MongoClient()
db_client = client[DB_NAME]

# test reading time series of data from a single gauge
results = gauge_data.get_gauge_data(db_client=db_client, station_id=STATION_ID, dates={
                                    'start': START_DATE, 'end': END_DATE})
for record in results:
    print(datetime.fromtimestamp(
        record['properties']['valid_time'],tz=timezone.utc), record['properties']['precipitation'])

client.close()
