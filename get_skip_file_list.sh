#!bin/bash

ls -1 data/skip/*.xbrl | awk -F/ '{print$NF}' > ./output/skip_list.txt

