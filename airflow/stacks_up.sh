#!/bin/bash

echo "😁 Setting temporary socket permissions..."
sudo chmod 666 /var/run/docker.sock

docker compose --profile flower up

