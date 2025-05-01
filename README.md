## ☠️ The Jolly Code — A Booty Huntin' Repo

![Pirate Approved](https://img.shields.io/badge/status-seaworthy-brightgreen?style=for-the-bilge&logo=skull)

![Arrrrrgh](hesapirate.png)

> 🏴‍☠️This here pipeline plunders metadata 'bout media from far 'n wide, then hauls it to me ship, swabs it clean, gives it a fine polish, and stows it in the hold—ready to please the cap’n’s every whim.🦜

---

### 🗺️ Map o' the Treasure
[Architecture]

### 🏴‍☠️ Ye Can See It Yonder
⚓ A demo be brewin’ in the depths. Come back when the tides be right!

### ⚓ What Be Afloat?
- ⚔️ Snappy to hoist 'n launch
- 💨 Fast as a cutlass in the moonlight
- 📜 Code so clean, even the Kraken can read it
- 🔐 Locked tighter than Davy Jones’ locker

### 🏗️ Settin’ Sail
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
- [x] Service worker
    - To be used with DockerOperator
    - [x] NOT working due to socket permissions. -> K8S would've handled it more gently -> Fixed
    - [x] Had to chmod the socket to 666 temporarily. -> must find better solution -> Nope just toggle chmod.
- [x] Sources sorter(for dynamic tasks) DAG
- [x] 1st ingestiong DAG MVP
- [x] Flower setup for worker monitoring
- [ ] 1st transformer test
- [ ] Publisher & Redis throttle

### ☠️ Cap’n’s Orders
- [ ] Alert message
- [ ] K8S Migration
- [ ] CI/CD
- [ ] Spark wave analysis, normalization, equilization, and so on

### 💀 From the Crow's Nest to the Cabin
- DockerOperator is a pain...
- [DockerOperator](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html)
- [DAG](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG)

### 📜 Code o' the Ship
This treasure be under the MIT License — share it, fork it, do what ye will... but give a nod to the cap’n.

> 🏴 Avast! May yer bugs be few and yer deploys be swift!