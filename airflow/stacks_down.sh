#!/bin/bash

COMPOSE_CONFIG=/lab/dee/repos_side/radeeo/airflow/docker-compose.yaml
echo "Listing all DAGs..."
ALL_DAGS=$(docker compose -f "$COMPOSE_CONFIG" exec airflow-scheduler airflow dags list --output json | jq -r '.[].dag_id')

# Wait for runs to be done..
# ..and pause dags
echo "Pausing all running DAGs..."
for DAG_ID in $ALL_DAGS; do
    running=1
    while [[ $running > 0 ]]; do
        output=$(
            docker compose -f "$COMPOSE_CONFIG" exec -T airflow-scheduler \
            airflow dags list-runs -d "$DAG_ID" --state running
        )
        if [[ "$output" == *"No data found"* ]]; then
            echo "No runs for $DAG_ID! Pausing..."
            docker compose -f "$COMPOSE_CONFIG" exec airflow-scheduler airflow dags pause "$DAG_ID" > /dev/null 2>&1
            running=0
        else
            echo "$DAG_ID still running. Will retry after 10s..."
            sleep 10
        fi
    done
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
    
