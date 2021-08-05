# rain_base
test some ideas on how to manage large sets of radar rainfall maps

Radar rainfall maps are stored in netCDF4 files and this project will evaluate how best to keep them in a mongodb file system.  
I will add metadata about the rainfall field, initially mean, standard deviation, wetted area ratio, and perhaps the quartiles of the raining pixels.  
I will look at two options:  
* write the entire file to the mongo database and add metadata  
* do some data compression using RLE of the zero pixels and keep the projection metadata in a separate collection, assuming one projection per domain ID.  
