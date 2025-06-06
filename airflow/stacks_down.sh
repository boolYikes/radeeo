#!/bin/bash

COMPOSE_CONFIG=/lab/dee/repos_side/radeeo/airflow/docker-compose.yaml
echo "Listing all DAGs..."
ALL_DAGS=$(docker compose -f "$COMPOSE_CONFIG" exec airflow-scheduler airflow dags list --output json | jq -r '.[].dag_id')

# Pause dags...
echo "Pausing all running DAGs..."
for DAG_ID in $ALL_DAGS; do
    docker compose -f "$COMPOSE_CONFIG" exec airflow-scheduler airflow dags pause "$DAG_ID" > /dev/null 2>&1
done

# ...and wait for runs to be done
while true; do
    running_tasks=0
    for DAG_ID in $ALL_DAGS; do
        output=$(
            docker compose -f "$COMPOSE_CONFIG" exec -T airflow-scheduler \
            airflow dags list-runs -d "$DAG_ID" --state running
        )
        if [[ "$output" != *"No data found"* ]]; then
            running_tasks=$((running_tasks + 1))
        fi
    done

    if [ "$running_tasks" -eq 0 ]; then
        echo "No running tasks left. Proceeding to terminate..."
        break
    fi
    echo "$running_tasks task(s) still running... waiting 10s"
    sleep 10
done

# Shut down Flower
echo "Shutting down Flower..."
docker compose -f "$COMPOSE_CONFIG" down flower

while true; do
    FLOWER_STAT=$(docker compose -f "$COMPOSE_CONFIG" ps flower | grep "Up")

    if [ -z "$FLOWER_STAT" ]; then
        echo "Flower is down."
        break
    else
        echo "Waiting for Flower to stop... 5 sec..."
        sleep 5
    fi
done

# Complete shutdown
echo "Shutting down everything else"
docker compose -f "$COMPOSE_CONFIG" down

echo "All done !"
    
