#!bin/bash

ls -1 data/skip/*.xbrl | awk -F/ '{print$NF}' > skip_list.txt

