# DAReflow
DAReflow is a prototype orchestration platform built to support the DARe Integrated Modelling Platform (IMP). It uses Apache Airflow to orchestrate interconnected modelling workflows, datasets, and data transformations.

![DAReflow Architecture](https://github.com/DAReHub/DAReflow/images/architecture.drawio.png?raw=true)

## About

DAReflow is a prototype orchestration platform developed to support the DARe Integrated Modelling Platform (IMP). It uses Apache Airflow to orchestrate interconnected modelling workflows, datasets, and data transformations.

An IMP combines multiple weather, hydrodynamic, heat, and agent-based transport models to investigate the impacts of extreme weather events on transportation networks. These models are typically developed independently and can differ substantially in their technologies, interfaces, computational requirements, and input/output formats. As a result, integrating them into a single workflow can require bespoke data transformations and manual configuration.

DAReflow addresses these challenges by providing a common orchestration layer around the models and their associated data. Airflow DAGs define the sequence of processing steps, while Docker containers provide a consistent environment for individual model applications.

### Architecture

Each model or processing step can be represented as a node within an Airflow DAG. Nodes can execute model applications, transform data, or perform data transfer operations. Model versions are containerised using Docker, allowing specific versions to be stored and subsequently retrieved when a workflow is executed.

Model outputs are stored as objects in MinIO, using its S3-compatible API, while PostgreSQL stores associated metadata. This separates large filesystem-based model outputs from the metadata required to manage and trace them.

Git provides version control for the orchestration system and its associated components, supporting reproducibility and historical tracking of modelling configurations.

### Key Components

| Component          | Purpose                                   |
| ------------------ | ----------------------------------------- |
| **Apache Airflow** | Workflow orchestration and execution      |
| **Docker**         | Containerisation of model applications    |
| **MinIO**          | Object storage for model data and outputs |
| **PostgreSQL**     | Metadata and relational data storage      |
| **Git**            | Version control and workflow history      |

DAReflow is a proof-of-concept rather than a complete implementation of the DARe IMP. It demonstrates how orchestration, containerisation, object storage, metadata management, and version control can be combined to support complex integrated modelling workflows.


## How to use this Repository

Run using Docker and follow the example DAGs included to structure your own.

### Run using Docker
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

### DAGs


### Specifying Inputs