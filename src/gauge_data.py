
import gridfs
from pymongo import MongoClient
# Function to read the rainfields gauge data and write it to the database 
import netCDF4 as nc
import os

# Write the data to the MongoDB
# returns None on error 
def load_gauge_data(**kwargs): 
    rf3_name = kwargs.get("file_path",None)
    db_client = kwargs.get("db_client",None)
    db_name = kwargs.get("db_name", None) 
    if db_client == None and db_name == None:
        print("Need one of db_name or db_client")
        return None

    if rf3_name ==  None: 
        print(f"file_path keyword not found")
        return None

    # open the ncfile 
    try:
        ds = nc.Dataset(rf3_name)
        print(ds)
        
    except FileNotFoundError : 
        print(f"Error: {rf3_name} not found")
        return None

    # # setup the database and write  
    # if db_name != None: 
    #     client = MongoClient()
    #     db_client = client[db_name]
    #     fs = gridfs.GridFSBucket(db_client) 
    #     file_id = fs.upload_from_stream(fname,buf,metadata={"name":"rain_rate","mean":mean,"stdev":std,"WAR":war})
    #     client.close()
    # else:
    #     fs = gridfs.GridFSBucket(db_client) 
    #     file_id = fs.upload_from_stream(fname,buf,metadata={"name":"rain_rate","mean":mean,"stdev":std,"WAR":war})

    print(f"written {rf3_name}")
    return 0

