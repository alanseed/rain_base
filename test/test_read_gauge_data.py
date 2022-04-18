""" Test the get_gauge_data function  

    Returns:
        _type_: _description_
"""
from datetime import datetime, timedelta
from pymongo import MongoClient
from package import gauge_data

START_DATE = datetime(2021, 12, 8, 8, 0)
END_DATE = datetime(2021, 12, 8, 23, 45)
TIME_STEP = timedelta(minutes=15)
DB_NAME = 'gaugeobs'
STATION_ID = 597506

# drop the existing data base for the test
client = MongoClient()
db_client = client[DB_NAME]
results = gauge_data.get_gauge_data(db_client=db_client, station_id=STATION_ID, dates={
                                    'start': START_DATE, 'end': END_DATE})
client.close()

for record in results:
    print(datetime.fromtimestamp(
        record['properties']['valid_time']), record['properties']['precipitation'])
