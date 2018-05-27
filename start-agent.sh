#!/bin/bash

ETCD_HOST=etcd
ETCD_PORT=2379
ETCD_URL=http://$ETCD_HOST:$ETCD_PORT

echo ETCD_URL = $ETCD_URL

if [[ "$1" == "consumer" ]]; then
  echo "Starting consumer agent..."
  java -jar \
       -Xms1536M \
       -Xmx1536M \
       -Dtype=consumer \
       -Dserver.port=20000 \
       -Detcd.url=$ETCD_URL \
       -Dlogs.dir=/root/logs \
       /root/dists/mesh-agent.jar
elif [[ "$1" == "provider-small" ]]; then
  echo "Starting small provider agent..."
  ./provider-agent-cjx \
        -etcdurl=$ETCD_URL \
        -agentPort=30000 \
        -dubboPort=20880 \
        -threadnum=3 \
        -logs=/root/logs
elif [[ "$1" == "provider-medium" ]]; then
  echo "Starting medium provider agent..."
    ./provider-agent-cjx \
        -etcdurl=$ETCD_URL \
        -agentPort=30000 \
        -dubboPort=20880 \
        -threadnum=3 \
        -logs=/root/logs
elif [[ "$1" == "provider-large" ]]; then
  echo "Starting large provider agent..."
    ./provider-agent-cjx \
        -etcdurl=$ETCD_URL \
        -agentPort=30000 \
        -dubboPort=20880 \
        -threadnum=3 \
        -logs=/root/logs
else
  echo "Unrecognized arguments, exit."
  exit 1
fi
