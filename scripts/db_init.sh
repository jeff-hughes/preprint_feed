#!/usr/bin/env bash

# Sets up the elasticsearch database (in docker) and initializes document
# schemas/mappings

# change to the root project directory
cd "$HOME/preprint_recommender"

# if local environment files exist, source them first
if [ -f ".env" ]; then
    source .env
fi

DO_MAPPING=true
SET_USERS=true

# handle command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --nomap)
            DO_MAPPING=false
            shift;;
        --nousers)
            SET_USERS=false
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

docker-compose up -d \
    && echo "Waiting for database to initialize..." \
    && sleep 60

if [[ "$SET_USERS" == true ]]; then
    ../scripts/db_set_users.sh
fi

if [[ "$DO_MAPPING" == true ]]; then
    python3 ./elastic_mapping.py
fi