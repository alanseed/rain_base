""" Accumulate the rainfall data and write the files into the database
"""
import sys
import getopt
import json

import os
import datetime
from pymongo import MongoClient, ASCENDING
from package import rain_base_fs

def usage():
    """_summary_ Generate the usage message
    """
    print(
        "Usage:\n --help to print this usage\n --input=filename.json to read the condig json file\n"
    )

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
    valid_time = datetime.datetime.fromisoformat(config["START_DATE"]).replace(tzinfo=datetime.timezone.utc)
    end_time = datetime.datetime.fromisoformat(config["END_DATE"]).replace(tzinfo=datetime.timezone.utc)
    in_time_step = datetime.timedelta(minutes=int(config["IN_TIME_STEP"]))
    out_time_step = datetime.timedelta(minutes=int(config["OUT_TIME_STEP"]))





if __name__ == "__main__":
    main()

