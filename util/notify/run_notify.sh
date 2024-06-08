#!/bin/bash

# run_notify.sh
# Runs the notifier utility script

cd /home/ubuntu/gas/util/notify
source /home/ubuntu/.virtualenvs/mpcs/bin/activate
/home/ubuntu/.virtualenvs/mpcs/bin/python /home/ubuntu/gas/util/notify/notify.py

### EOF