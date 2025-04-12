### TODOs
- K8S Migration
- CI/CD

### Setup
- [x] Clickhouse: [LINK](https://hub.docker.com/_/clickhouse)
    - Create volumes
    - Run:
        ```bash
        docker run -d --rm \
         -v "/lab/clickhouse/var/lib/clickhouse:/var/lib/clickhouse/" \
         -v "/lab/clickhouse/var/log/clickhouse-server:/var/log/clickhouse-server/" \
         --network=host \
         --name clickhouse --ulimit nofile=262144:262144 clickhouse
        ```
        To connect to `--network=host`, accessing containers must be run with `--add-host=host.docker.internal:host-gateway` option or add under `extra_hosts:` section under service as a list: `"host.docker.internal:host-gateway"` if using docker compose
        *Other volumes*
        ```bash
        -v "/lab/clickhouse/docker-entrypoint-initdb.d:/docker-entrypoint-initdb.d/" \
        -v "/lab/clickhouse/etc/clickhouse-server/config.d/config.xml:/etc/clickhouse-server/config.xml" \
        -v "/lab/clickhouse/etc/clickhouse-server/users.d:/etc/clickhouse-server/users.d/" \
        ```

- [x] Airflow: [LINK](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)