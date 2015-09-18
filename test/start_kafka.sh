#!/bin/sh
docker run --net=host -e HOSTNAME=0.0.0.0 --name=kafka-conduit-test tobegit3hub/standalone-kafka
