#!/bin/bash
echo -e "Starting \033[4mClickhouse\033[0m container..."

cont_name="clickhouse"

if ! docker ps -a --format '{{.Names}}' | grep -wq "$cont_name"; then
	docker run -d --rm \
		-v "/lab/clickhouse/var/lib/clickhouse:/var/lib/clickhouse/" \
		-v "/lab/clickhouse/var/log/clickhouse-server:/var/log/clickhouse-server/" \
		--network=host \
		--name "$cont_name" --ulimit nofile=262144:262144 clickhouse
else
	echo "Container $cont_name exists."
	if [ "$(docker inspect -f '{{.State.Status}}' '$cont_name' 2>/dev/null)" = "exited" ]; then
		docker start "$cont_name"
	fi
fi
