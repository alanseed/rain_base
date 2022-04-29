#!/bin/bash
BASE_DIR="/home/awseed/data/RF3/prcp-c5/71/2022"
for file in "$BASE_DIR"/*.tar
do 
    date=${file: -20}
    year=${date:0:4}
    month=${date:4:2}
    day=${date:6:2}
    path=${BASE_DIR}"/"${month}"/"${day}
    mkdir -p $path 
    tar -xvf ${file} -C ${path} 
done     
