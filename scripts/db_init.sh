#!/usr/bin/env bash

# change to the root project directory (up one level from current script dir)
cd "$( dirname "${BASH_SOURCE[0]}" )"
cd ".."

# if local environment files exist, source them first
if [ -f ".env" ]; then
    source .env
fi

DO_MAPPING=true

# handle command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --nomap)
            DO_MAPPING=false
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


# set up elasticsearch in docker container, and create mapping schemas
# if necessary
cd "elastic"
if [ "$DO_MAPPING" = true ]; then
    docker-compose up -d \
        && sleep 30 \
        && python3 ./elastic/elastic_mapping.py
else
    docker-compose up -d
fi