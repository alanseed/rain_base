
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

def write_to_rain_basefs(rf3_name): 
    # read the ncfile into memory 
    nc_file = open(rf3_name,"rb")
    buf = nc_file.read() 
    nc_file.close()

    # get the metadata
    fname = os.path.basename(rf3_name)
    mean, std, war = get_field_stats(buf)
    
    # setup the database and write  
    client = MongoClient()
    rain_base = client["rain_base_fs"]
    fs = gridfs.GridFSBucket(rain_base) 
    file_id = fs.upload_from_stream(fname,buf,metadata={"mean":mean,"stdev":std,"WAR":war})
    client.close()
    return 

