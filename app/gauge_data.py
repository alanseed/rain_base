"""Module to read and write gauge data to the database next version

    Returns:
        _type_: _description_
"""
from datetime import datetime
from pymongo import MongoClient
from netCDF4 import Dataset

def load_gauge_data(**kwargs):
    """Reads a rainfields3 rain gauge netcdf file and writes the data to a "gauge_data" collection
    in a MongoDB database
    kwargs:
        file_path: path to the rf3 gaugeobs file to be read
        db_client: MongoDB client for the output database
        db_name: Name of the MongoDB collection for the output database
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
    rf3_start_time = rf3_ds['start_time'][0].item()
    rf3_valid_time = rf3_ds['valid_time'][0].item()

    station_id = rf3_ds['station_id'][:]
    latitude = rf3_ds['latitude'][:]
    longitude = rf3_ds['longitude'][:]
    precipitation = rf3_ds['precipitation'][:]
    latest_obs_time = rf3_ds['latest_obs_time'][:]

    # loop over the stations and load up the gauge_list
    gauge_list = []
    for station in range(1, number_stations):
        geojson_record = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [longitude[station].item(), latitude[station].item()],
            },
            'properties': {
                'start_time': rf3_start_time,
                'valid_time': rf3_valid_time,
                'station_id': station_id[station].item(),
                'precipitation': precipitation[station].item(),
                'latest_obs_time': latest_obs_time[station].item(),
            }
        }
        gauge_list.append(geojson_record)

    # open the connection to the database if required
    if gauge_db_name is not None:
        myclient = MongoClient()
        gauge_db_client = myclient[gauge_db_name]
    gauge_collection = gauge_db_client["gauge_data"]

    # write all the data
    gauge_collection.insert_many(gauge_list)

    # close the connection if required
    if gauge_db_name is not None:
        myclient.close()

    print(f"written {rf3_name}")
    return 0


def get_gauge_data(**kwargs):
    """Read gauge data from the MongoDB collection
    kwargs:
        db_client: MongoDB client for the rain gauge database

        location: for a search on a station id {'station_id': ID of the station}, OR
                  for a search on a location {'geometry': geoJSON Point location object,
                                              'maxDistance': maximum distance in m for the search }
        dates:{'start_time':UTC start time as datetime,
               'end_time':UTC end time as datetime}

    Returns:
        MongoDB cursor object sorted on valid_time and station_id
    """

    # check that we have all the key words
    dates = kwargs.get("dates", None)
    if dates is None:
        print("dates keyword not found")
        return None

    search_location = kwargs.get("location", None)
    if search_location is None:
        print("location key word not found")
        return None

    gauge_db_client = kwargs.get("db", None)
    if gauge_db_client is None:
        print("db keyword not found")
        return None

    query = {}
    query['properties.valid_time'] = {'$gte': int(datetime.timestamp(
        dates['start'])), '$lte': int(datetime.timestamp(dates['end']))}

    if 'station_id' in search_location.keys():
        query['properties.station_id'] = search_location['station_id']
    else:
        query['geometry'] = {
            '$nearSphere': {
                '$geometry': search_location['geometry'],
                '$maxDistance': search_location['maxDistance']}
        }

    cursor = gauge_db_client["gauge_data"].find(
        query).sort([("properties.valid_time",1), ("properties.station_id",1)])

    return cursor
