
import gridfs
from pymongo import MongoClient
from netCDF4 import Dataset 
import os

# function to read the nc file and get some field stats to use as metadata 
def get_field_stats(buf):
    ncFile = Dataset("fname", mode="r", memory=buf)
    scaled_rain = ncFile['rain_rate'][:]
    scale_factor = ncFile.variables['rain_rate'].scale_factor 
    rain = scaled_rain * scale_factor 
    
    mean = rain.mean()
    std = rain.std()
    rain_area = (rain > 0.1).sum()
    valid_area = rain.count()
    war = 100 * rain_area / valid_area 
    
    return mean, std, war 

# Write the data to the MongoDB
# returns None on error 
def write_to_rain_basefs(**kwargs): 
    rf3_name = kwargs.get("file_path",None)
    db_client = kwargs.get("db_client",None)
    db_name = kwargs.get("db_name", None) 
    if db_client == None and db_name == None:
        print("Need one of db_name or db_client")
        return None

    if rf3_name ==  None: 
        print(f"file_path keyword not found")
        return None

    # read the ncfile into memory 
    try:
        nc_file = open(rf3_name,"rb")
        buf = nc_file.read() 
        nc_file.close()
    except FileNotFoundError : 
        print(f"Error: {rf3_name} not found")
        return None

    # get the metadata
    fname = os.path.basename(rf3_name)
    mean, std, war = get_field_stats(buf)
    
    # setup the database and write  
    if db_name != None: 
        client = MongoClient()
        db_client = client[db_name]
        fs = gridfs.GridFSBucket(db_client) 
        file_id = fs.upload_from_stream(fname,buf,metadata={"name":"rain_rate","mean":mean,"stdev":std,"WAR":war})
        client.close()
    else:
        fs = gridfs.GridFSBucket(db_client) 
        file_id = fs.upload_from_stream(fname,buf,metadata={"name":"rain_rate","mean":mean,"stdev":std,"WAR":war})

    print(f"written {rf3_name}")
    return file_id

