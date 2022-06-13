#!/bin/bash
BASE_DIR="/home/awseed/data/RF3/gndrefl/71/2022"
for file in "$BASE_DIR"/*.zip
do 
    date=${file: -20}
    echo $date
    year=${date:0:4}
    month=${date:4:2}
    day=${date:6:2}
    path=${BASE_DIR}"/"${month}"/"${day}
    mkdir -p $path 
    unzip -q ${file} -d ${path} 
done     
