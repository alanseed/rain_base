""" Accumulate the rainfall data and write the files into the database
"""
import sys
import getopt
import json
import datetime
import io
import numpy as np

from netCDF4 import Dataset
from pymongo import MongoClient, ASCENDING
import gridfs as fs


def usage():
    """_summary_ Generate the usage message
    """
    print(
        "Usage:\n --help to print this usage\n --input=filename.json to read the condig json file\n"
    )


def make_file_name(stn_id, product, valid_time):
    """Make the rf3 file name for database
    """
    ymd = valid_time.strftime("%Y%m%d")
    hms = valid_time.strftime("%H%M%S")
    file_name = f"{stn_id}_{ymd}_{hms}.{product}.nc"
    return file_name


def main():
    """ Main function
    """
    # parse the command line inputs
    short_options = "hi:"
    long_options = ["help", "input="]
    full_cmd_arguments = sys.argv
    argument_list = full_cmd_arguments[1:]
    if len(argument_list) == 0:
        usage()
        sys.exit()

    try:
        options, arguments = getopt.getopt(
            argument_list, short_options, long_options)
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    input = None
    for option, parameter in options:
        if option in ("-h", "--help"):
            usage()
            sys.exit()
        elif option in ("-i", "--input"):
            input = parameter
            print(f"Input config file = {input}")
        else:
            assert False, "invalid option"

    if input is None:
        print("Input configuration file is required")
        usage()
        sys.exit()

    # read in the config file
    file = open(input, "r")
    config = json.load(file)
    file.close()

    # get the start and end dates and the time step for the input data
    start_time = datetime.datetime.fromisoformat(
        config["START_DATE"]).replace(tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime.fromisoformat(
        config["END_DATE"]).replace(tzinfo=datetime.timezone.utc)
    in_time_step = datetime.timedelta(minutes=int(config["IN_TIME_STEP"]))
    out_time_step = datetime.timedelta(minutes=int(config["OUT_TIME_STEP"]))

    # set up the database
    client = MongoClient()
    db = client[config["DB_NAME"]]
    radar_fs = fs.GridFS(db)

    start_accum = start_time
    while start_accum < end_time:
        end_accum = start_accum + out_time_step
        in_time = start_accum + in_time_step
        count = int(0)
        has_data = True

        # set up the output file for this accumulation
        out_file_name = make_file_name(
            config["STN_ID"], config["OUT_PRODUCT"], end_accum)
        ds_out = Dataset("./temp_accum_file.nc", mode="w",clobber=True)

        # start the loop over the 5 min files in this accumulation period
        while in_time <= end_accum:

            # make the name of the input file and get it from the database
            in_file_name = make_file_name(
                config["STN_ID"], config["IN_PRODUCT"], in_time)  
            file = radar_fs.find_one({"filename": in_file_name})

            if file is None:
                has_data = False
                print(f"ERROR: {in_file_name} not found")
                break

            data = file.read()
            ds_in = Dataset(in_file_name, mode="r", memory=data)

            if count == 0:

                # get the shape of the data array and create the accumulation array
                number_rows = ds_in.dimensions['y'].size
                number_cols = ds_in.dimensions['x'].size
                data_shape = (number_rows, number_cols)
                accum_rain = np.zeros(data_shape, dtype=np.float64)
                start_accum_time = int(ds_in['start_time'][0].item())

                # use the first input file as a template for the output file
                # Copy the global attributes
                ds_out.setncatts(ds_in.__dict__)

                # Copy dimensions
                for name, dim in ds_in.dimensions.items():
                    ds_out.createDimension(
                        name, len(dim) if not dim.isunlimited() else None)

                # Copy variables
                for v_name, in_var in ds_in.variables.items():
                    out_var = ds_out.createVariable(
                        v_name, in_var.datatype, in_var.dimensions)
                    out_var.setncatts({k: in_var.getncattr(k)
                                       for k in in_var.ncattrs()})
                    out_var[:] = in_var[:]

            # accumulate the rainfall
            in_rain = ds_in["precipitation"][:]
            valid_time = ds_in["valid_time"][:]
            accum_rain = np.add(accum_rain, in_rain)
            ds_in.close()

            in_time += in_time_step
            count += 1

        # write out the accumulation if all the input fields are found
        if has_data:
            ds_out["start_time"][:] = start_accum_time
            ds_out["valid_time"][:] = valid_time
            ds_out["precipitation"][:] = accum_rain

            # get the metadata for the output field
            my_meta_data = {}
            my_meta_data["station_id"] = int(ds_out.__getattr__("station_id"))
            my_meta_data["station_name"] = str(
                ds_out.__getattr__("station_name"))
            my_meta_data['valid_time'] = int(ds_out['valid_time'][0].item())
            my_meta_data['start_time'] = int(ds_out['start_time'][0].item())
            my_meta_data['variable'] = "precipitation"
            my_meta_data["product"] = config["OUT_PRODUCT"]

            my_meta_data["mean"] = accum_rain.mean()
            my_meta_data["std"] = accum_rain.std()
            rain_area = (accum_rain >= 0.05).sum()

            # TO DO - allow for missing data in the accum array
            valid_area = number_cols*number_rows
            my_meta_data["war"] = 100 * rain_area / valid_area

            # make a file-like object pointing to the output nc file memory buffer
            out_buf = io.BytesIO(ds_out.close())

            out_fs = fs.GridFSBucket(db)
            file_id = out_fs.upload_from_stream(
                out_file_name, out_buf, metadata=my_meta_data)
            if file_id is not None:
                print(f"written {out_file_name}")
        else:
            ds_out.close()

        start_accum += out_time_step
        accum_rain = None

    client.close()


if __name__ == "__main__":
    main()
