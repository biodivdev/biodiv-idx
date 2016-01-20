#!/bin/bash

docker-compose up -d taxadata
docker-compose up -d dwc
docker-compose up -d dwcbot
sleep 5
docker-compose up -d elasticsearch
sleep 15
docker-compose up -d kibana
docker-compose up biodividx

