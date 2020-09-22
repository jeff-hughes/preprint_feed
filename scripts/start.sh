#!/usr/bin/env bash

# change to the root project directory (up one level from current script dir)
cd "$( dirname "${BASH_SOURCE[0]}" )"
cd ".."

# if local environment files exist, source them first
if [ -f ".env" ]; then
    source .env
fi

LOAD_DATA=true

# handle command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --noloaddata)
            LOAD_DATA=false
            shift;;
        -?*)
            echo "ERROR: Unknown option: $1"
            exit 1
            shift;;
        *)
            break
   esac
   shift  # expose next parameter
done


# make sure Python has all necessary packages installed
if [ -f "requirements.txt" ]; then
    echo "Installing necessary Python packages..."
    pip3 install -r requirements.txt
fi

# this is all for now, but once we have an app to start up, the whole
# startup process will go here as well
if [ "$LOAD_DATA" = true ]; then
    # initialize database, then load historical arXiv data
    echo "Initializing database and loading data..."
    ./scripts/db_init.sh \
        && python3 load_arxiv_historical.py
else
    # this would be useful for restarting the app, where we still
    # have data in the database
    echo "Initializing database..."
    ./scripts/db_init.sh --nomap
fi