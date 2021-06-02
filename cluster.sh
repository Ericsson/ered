#!/bin/sh

docker run --name redis-1 -d --net=host redis redis-server --cluster-enabled yes --port 30001
docker run --name redis-2 -d --net=host redis redis-server --cluster-enabled yes --port 30002
docker run --name redis-3 -d --net=host redis redis-server --cluster-enabled yes --port 30003
docker run --name redis-4 -d --net=host redis redis-server --cluster-enabled yes --port 30004
docker run --name redis-5 -d --net=host redis redis-server --cluster-enabled yes --port 30005
docker run --name redis-6 -d --net=host redis redis-server --cluster-enabled yes --port 30006

sleep 5
echo 'yes' | docker run --name redis-cluster --net=host -i redis redis-cli --cluster create 127.0.0.1:30001 127.0.0.1:30002 127.0.0.1:30003 127.0.0.1:30004 127.0.0.1:30005 127.0.0.1:30006 --cluster-replicas 1
