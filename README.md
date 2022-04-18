# rain_base
Set of python scripts to write netCDF radar data to the MongoDB along with meta data 



## Installation
Install the package using `python setup.py install` 

## Testing 
`test_load_gauge_data.py`  
Reads a set of Rainfields3 gaugeobs netcdf files and loads them into a mongo database  
The mongo db client is assumed to be local and at the default address  
