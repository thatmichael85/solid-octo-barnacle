#!/bin/bash

# Function to start or create and start a container
start_container() {
    local container_name=$1
    local image=$2
    local ports=$3
    local env_vars=$4
    local volumes=$5
    local network=$6
    local extra_commands=$7
    
    echo Create folder if not exist
    mkdir -p $container_name
    # Check if the container already exists
    if podman container exists $container_name; then
        echo "Container $container_name exists. Starting if not running..."
        podman start $container_name
    else
        echo "Creating and starting container $container_name..."
        podman run -d --name $container_name $ports $env_vars $volumes $network $image $extra_commands
    fi
}

# Create network if it doesn't exist
# podman network exists mongo-network || podman network create mongo-network

# Start or create containers
# LocalStack
# start_container \
#     "localstack-main" \
#     "localstack/localstack" \
#     "-p 4566:4566 -p 4510-4559:4510-4559" \
#     "-e DEBUG=1" \
#     "-v ${PWD}/volume:/var/lib/localstack -v /var/run/docker.sock:/var/run/docker.sock" \
#     "--network mongo-network"


# MongoDestination
start_container \
    "mongodestination" \
    "mongo:6.0.12" \
    "-p 27018:27018" \
    "-e MONGO_INITDB_ROOT_USERNAME=destUsername -e MONGO_INITDB_ROOT_PASSWORD=destPassword" \
    "-v ${PWD}/mongodestination:/data/db" \
    "--network host" \
    "--port 27018"
sleep 2

# Second MongoExpress
start_container \
    "mongo-express2" \
    "mongo-express" \
    "-p 8082:8082" \
    "-e PORT=8082 -e ME_CONFIG_BASICAUTH_USERNAME=root -e ME_CONFIG_BASICAUTH_PASSWORD=example -e ME_CONFIG_MONGODB_URL=mongodb://destUsername:destPassword@localhost:27018/" \
    "--network host"

# MongoSource
start_container \
    "mongosource" \
    "mongo:4.4" \
    "-p 27017:27017" \
    "-e MONGO_INITDB_ROOT_USERNAME=sourceUsername -e MONGO_INITDB_ROOT_PASSWORD=sourcePassword" \
    "-v ${PWD}/mongosource:/data/db" \
    "--network host"
sleep 2
# MongoExpress
start_container \
    "mongo-express" \
    "mongo-express" \
    "-p 8081:8081" \
    "-e ME_CONFIG_BASICAUTH_USERNAME=root -e ME_CONFIG_BASICAUTH_PASSWORD=example -e ME_CONFIG_MONGODB_URL=mongodb://sourceUsername:sourcePassword@localhost:27017/" \
    "--network host"

echo "All containers are set up."