#!/bin/bash

set -e

envsubst < /opt/mm2/connect-standalone.properties > /var/run/mm2/connect-standalone.properties
envsubst < /opt/mm2/mirror-checkpoint-connector.properties > /var/run/mm2/mirror-checkpoint-connector.properties
envsubst < /opt/mm2/mirror-heartbeat-connector.properties > /var/run/mm2/mirror-heartbeat-connector.properties
envsubst < /opt/mm2/mirror-source-connector.properties > /var/run/mm2/mirror-source-connector.properties

/opt/bitnami/kafka/bin/connect-standalone.sh \
  /var/run/mm2/connect-standalone.properties  \
  /var/run/mm2/mirror-heartbeat-connector.properties \
  /var/run/mm2/mirror-checkpoint-connector.properties \
  /var/run/mm2/mirror-source-connector.properties