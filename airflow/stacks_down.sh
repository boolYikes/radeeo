#!/bin/bash

COMPOSE_CONFIG=/lab/dee/repos_side/radeeo/airflow/docker-compose.yaml
echo "Listing all DAGs..."
ALL_DAGS=$(docker compose -f "$COMPOSE_CONFIG" exec airflow-scheduler airflow dags list --output json | jq -r '.[].dag_id')

# Pause all listed DAGs
for DAG_ID in $ALL_DAGS; do
    echo "Pausing DAG: $DAG_ID"
    docker compose -f "$COMPOSE_CONFIG" exec airflow-scheduler airflow dags pause "$DAG_ID"
done

# Wait for all DAGs to finish current runs
echo "Checking for running dags before termination..."
while true; do
    ANY_RUNNING=false

    for DAG_ID in $ALL_DAGS; do
        RUNNING_OUTPUT=$(docker compose -f "$COMPOSE_CONFIG" exec airflow-scheduler airflow dags list-runs -d "$DAG_ID" --state running)

        if ! echo "$RUNNING_OUTPUT" | grep -q "No data found"; then
            echo "DAG $DAG_ID still running..."
            ANY_RUNNING=true
        fi
    done

    if [ "$ANY_RUNNING" = false ]; then
        echo "No running DAGs detected. Proceeding to terminate..."
        break
    else
        echo "Some DAGs still running... will check 5 sec later"
        sleep 5
    fi
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
    