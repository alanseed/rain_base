# **rain_base**
Set of python scripts to write netCDF radar data to the MongoDB along with meta data 


## Installation
The conda environment is found in `rain_base.yml` and currently uses python v3.10  
Once the rain_base environment is set  install the `package` module using `pip install .` from the `rain_base` directory  

## Testing 
`test_load_gauge_data.py`  
Reads a set of Rainfields3 gaugeobs netcdf files and loads them into a mongo database  
The mongo db client is assumed to be local and at the default address  

`test_read_data.py`  
Tests reading a time series of data from rain gauge  

## Applications  
The applications are found in the `app` directory  

### **Loading gauge data into data base**  

`load_gauge_data.py`  Application reads Rainfields3 gaugeobs netcdf files and loads the data into a database  
`test.json`  An example of the input configuration file for load_gauge_data.py 