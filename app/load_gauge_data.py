""" Optionally create new database and Load gauge data from an input directory 
"""
import sys
import getopt
import json

import os
import datetime
from pymongo import MongoClient, ASCENDING
from package import gauge_data


def usage():
    """_summary_ Generate the usage message
    """
    print(
        "Usage:\n --help to print this usage\n --input=filename.json to read the condig json file\n"
    )


def main():
    """_summary_ Run the main script
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

    # drop the database if required and open the connection
    client = MongoClient()
    if config["DROP_DATABASE"] == "1":
        dbnames = client.list_database_names()
        if config["DB_NAME"] in dbnames:
            client.drop_database(config["DB_NAME"])
    db_client = client[config["DB_NAME"]]

    # loop over the dates and read in the data
    valid_time = datetime.datetime.fromisoformat(config["START_DATE"])
    end_time = datetime.datetime.fromisoformat(config["END_DATE"])
    time_step = datetime.timedelta(minutes=int(config["TIME_STEP"]))
    while valid_time <= end_time:
        y = valid_time.strftime("%Y")
        ymd = valid_time.strftime("%Y%m%d")
        hms = valid_time.strftime("%H%M%S")
        file_path = os.path.join(
            config["BASE_DIR"], valid_time.strftime("%Y/%m/%d/"))
        file_name = f"{config['STN_ID']}_{ymd}_{hms}.{config['PRODUCT']}"
        file_path = os.path.join(file_path, file_name)
        print(f"Reading {file_path}")
        file_id = gauge_data.load_gauge_data(
            file_path=file_path, db_client=db_client)
        if file_id is None:
            break

        valid_time = valid_time + time_step

# make the indexes if the database has been dropped
    if config["DROP_DATABASE"] == "1":
        resp = db_client["gauge_data"].create_index(
            [('properties.valid_time', ASCENDING)], name='ValidTimeIndex')
        resp = db_client["gauge_data"].create_index(
            [('geometry', '2dsphere')], name='LocationIndex')

    client.close()


if __name__ == "__main__":
    main()