# DAReflow
Apache airflow implementation for processing and chaining models

## Docker
Initial build:

    docker compose build
    docker compose up airflow-init
    docker compose up -d

Update to requirements:

    docker compose down
    docker compose build
    docker compose up airflow-init
    docker compose up -d

Reload airflow and dependencies while keeping persistent data (e.g. an update to environment variables):

    docker compose down
    docker compose up airflow-init
    docker compose up -d