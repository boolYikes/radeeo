#!/bin/bash

dwon

echo "😁 Setting temporary socket permissions..."
sudo chmod 666 /var/run/docker.sock

docker compose -f /lab/dee/repos_side/radeeo/airflow/docker-compose.yaml --profile flower up -d
