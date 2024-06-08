#!/bin/bash

# run_thaw_app.sh
# Runs the Glacier thawing utility Flask app

export THAW_APP_HOME=/home/ubuntu/gas/util/thaw
export SOURCE_HOST=0.0.0.0
export HOST_PORT=5002

cd $THAW_APP_HOME

/home/ubuntu/.virtualenvs/mpcs/bin/uwsgi \
    --chdir $THAW_APP_HOME \
    --enable-threads \
    --http $SOURCE_HOST:$HOST_PORT \
    --log-master \
    --manage-script-name \
    --mount /thaw_app=thaw_app:app \
    --socket /tmp/thaw_app.sock \
    --vacuum

### EOF