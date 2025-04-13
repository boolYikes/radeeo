#!/bin/bash
echo -e "Starting \033[4mClickhouse\033[0m container..."
docker run -d --rm \
	-v "/lab/clickhouse/var/lib/clickhouse:/var/lib/clickhouse/" \
	-v "/lab/clickhouse/var/log/clickhouse-server:/var/log/clickhouse-server/" \
	--network=host \
	--name clickhouse --ulimit nofile=262144:262144 clickhouse
