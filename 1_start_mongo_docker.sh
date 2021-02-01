#!/bin/sh

docker stack deploy -c /mongo_stack.yml mongodb
