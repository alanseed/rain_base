# rain_base
Set of python scripts to write netCDF radar data to the MongoDB along with meta data 



## Installation
Install the package module using `pip install -e .` from the rain_base directory

## Testing 
`test_load_gauge_data.py`  
Reads a set of Rainfields3 gaugeobs netcdf files and loads them into a mongo database  
The mongo db client is assumed to be local and at the default address  

`test_read_data.py`  
Tests reading a time series of data from rain gauge  
