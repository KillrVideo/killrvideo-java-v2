#!/bin/bash

# This is a simple helper script that creates a Docker image of the 
# killrvideo-java application to run in "headless" mode.
# Just pass in the Docker image version you want as a parameter to the 
# script "./create-headless-server.sh 2.1.0" and it will create an 
# image with that version. If you leave it out it will default to "latest".
version=$1

if [[ -z "$1" ]]; then
  version="latest"
fi

docker build -t killrvideo/killrvideo-java-server:$version .

# I wasn't kidding, it's really simple