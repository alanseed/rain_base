"""Module to read and write gauge data to the database 

    Returns:
        _type_: _description_
"""
from pymongo import MongoClient
from netCDF4 import Dataset
from geojson import Point

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
    rf3_start_time = rf3_ds['start_time'][0].item()
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
            'start_time': rf3_start_time,
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
