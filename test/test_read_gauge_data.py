""" Test the get_gauge_data function  

    Returns:
        _type_: _description_
"""
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from package import gauge_data

START_DATE = datetime(2021, 12, 7, 21, 0, tzinfo=timezone.utc)
END_DATE = datetime(2021, 12, 8, 0, 0, tzinfo=timezone.utc)

TIME_STEP = timedelta(minutes=15)
DB_NAME = 'gaugeobs'
STATION_ID = 597506
SEARCH_RANGE = 150000

# open the connection to the database
client = MongoClient()
db_client = client[DB_NAME]

# test reading time series of data from a single gauge
dates = {'start': START_DATE, 'end': END_DATE}

search_station_id = {
    'station_id': STATION_ID
}

results = gauge_data.get_gauge_data(
    db=db_client, location=search_station_id, dates=dates)


for record in results:
    print(datetime.fromtimestamp(
        record['properties']['valid_time'], tz=timezone.utc), record['properties']['precipitation'])

# test reading time series from a location

search_location = {
    'geometry': {'type': 'Point', 'coordinates': [151.0, -33.75]},
    'maxDistance': SEARCH_RANGE
}

print("testing search on location")
results = gauge_data.get_gauge_data(
    db=db_client, location=search_location, dates=dates)

for record in results:
    print(
        record['properties']['station_id'],
        datetime.fromtimestamp(
            record['properties']['valid_time'], tz=timezone.utc),
        record['properties']['precipitation'])

client.close()
