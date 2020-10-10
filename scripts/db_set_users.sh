#!/usr/bin/env bash

# Sets up user accounts in the elasticsearch database

# change to the root project directory
cd "$HOME/preprint_recommender"

# if local environment files exist, source them first
if [ -f ".env" ]; then
    source .env
fi

# create role for read-only access
curl -u $ELASTIC_ADMIN_USER:"$ELASTIC_ADMIN_PASS" -X POST "$ELASTIC_HOST/_xpack/security/role/app_ro" -H "Content-Type: application/json" -d '{ "cluster": ["monitor"], "indices": [{ "names": "*", "privileges": ["read", "monitor"] }] }'

# create read-only user
curl -u $ELASTIC_ADMIN_USER:"$ELASTIC_ADMIN_PASS" -X POST "$ELASTIC_HOST/_xpack/security/user/$ELASTIC_RO_USER" -H "Content-Type: application/json" -d '{ "password": "$ELASTIC_RO_PASS", "roles": ["app_ro"] }'
