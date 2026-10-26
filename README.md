# DAReflow
DAReflow is a prototype orchestration platform built to support the DARe Integrated Modelling Platform (IMP). It uses Apache Airflow to orchestrate interconnected modelling workflows, datasets, and data transformations.

![DAReflow Architecture](https://github.com/DAReHub/DAReflow/blob/main/images/architecture.drawio.png?raw=true)

## About

An IMP combines multiple weather, hydrodynamic, heat, and agent-based transport models to investigate the impacts of extreme weather events on transportation networks. These models are typically developed independently and can differ substantially in their technologies, interfaces, computational requirements, and input/output formats. As a result, integrating them into a single workflow can require bespoke data transformations and manual configuration.

DAReflow addresses these challenges by providing a common orchestration layer around the models and their associated data. Airflow DAGs define the sequence of processing steps, while Docker containers provide a consistent environment for individual model applications.

### Architecture

Each model or processing step can be represented as a node within an Airflow DAG. Nodes can execute model applications, transform data, or perform data transfer operations. Model versions are containerised using Docker, allowing specific versions to be stored and subsequently retrieved when a workflow is executed.

In this case, MinIO is ran alongside Airflow to mimic an AWS object storage location on the cloud. Model outputs are stored as objects in MinIO using its S3-compatible API. If you are not running MinIO, create a new connection to your AWS instance from within Airflow GUI, specifying Conn Type as aws. PostgreSQL stores associated metadata to separate large filesystem-based model outputs from the metadata required to manage and trace them.

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


## Setup and Run
### Files to Configure
#### docker-compose.yaml
The services provided with docker-compose.yaml are largely standard for an Airflow instance. However, some changes have been made including modifications to mounts. 

Additionally added services include:
- **docker-proxy**: Allows host Docker information to be known from within Airflow container.
- **reverse-proxy**: An NGINX instance to deploy Airflow on a trusted and secure network.

#### .env
Please see `example.env` to construct your `.env` file. Many of the variables included are not secrets but data input/output paths which point to locations within model containers.

**Note: An input and output directory should be created on host for each model, and referenced in .env where variables begin with `HOST_`**

### Airflow Connections and Variables
As an Airflow admin user you can define Connections and Variables from within the Airflow GUI (under Admin settings) and reference these variables within DAGs. These are not automatically populated when building this Airflow instance and must be made manually.

![Connections](https://github.com/DAReHub/DAReflow/blob/main/images/connections.png?raw=true)
The database connections used in this repository.

![Variables](https://github.com/DAReHub/DAReflow/blob/main/images/variables.png?raw=true)
The variables used in this repository.


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

## What's Already Included
This repository only acts as a demonstrator for how an Integrated Modelling Platform can be orchestrated. Therefore, only a slice of the DARe IMP is represented. Build your own orchestrated IMP with your own models, using the DAGs in this repository as a template.

### DAGs
DAReflow represents individual modelling workflows as Apache Airflow DAGs. Each DAG defines the sequence of operations required to configure, execute, and manage a model, including input staging, model execution, output handling, and metadata recording.

The repository contains example DAGs demonstrating how individual models can be integrated into the DAReflow orchestration framework.

| DAG                      | Description                                                |
|--------------------------|------------------------------------------------------------|
| MATSim_DAG.py            | Executes a MATSim transport simulation                     |
| PT2matsim_DAG.py         | Executes PT2-MATSim public transport processing            |
| CityCAT_DAG.py           | Executes a CityCAT hydrodynamic simulation                 |
| UDM_DAG.py               | Executes Urban Development Model                           |
| floodEvent_DAG.py        | Executes DARe floodEvent module code                       |
| floodEvent_MATSim_DAG.py | Executes DARe floodEvent and MATSim DAGs in sequence |

#### MATSim DAG

![MATSim DAG Graph](https://github.com/DAReHub/DAReflow/blob/main/images/MATSim_DAG_graph.png?raw=true)

The MATSim DAG demonstrates the typical workflow used by DAReflow:

1. Record the start of the run in PostgreSQL.
2. Create temporary input and output directories.
3. Retrieve input data from MinIO.
4. Configure the MATSim scenario.
5. Select and prepare the required Docker image.
6. Execute MATSim using the selected container.
7. Periodically upload generated outputs to MinIO.
8. Upload the final outputs to MinIO.
9. Record metadata for the completed run.
10. Determine the final DAG run state.

Model parameters are presented to the user when a DAG is triggered and can also be populated dynamically when DAGs are executed sequentially.

MATSim Docker images are selected using <name:tag>. Images not already available on the Airflow host are retrieved from MinIO and loaded into Docker, allowing users to upload and deploy model images independently of the orchestration host.

Because model runs can be long-running, output data is periodically copied to MinIO while the model is executing. This preserves intermediate results in the event of a failure. Post-processing and run-state tasks use ALL_DONE trigger rules so that execution status and available outputs can still be recorded when an upstream task fails.

Run metadata, including configuration, Docker image, Git commit, task states, and output location, is stored in PostgreSQL to support both operational monitoring and model provenance.

#### Combined DAGs
floodEvent_MATSim_DAG.py is an example of executing DAGs in a sequence, in this case running  floodEvent_DAG.py then MATSim_DAG.py. The graph representation here appears much simpler, only showing a single node operator for each DAG. Each node operator acts as a parent and remote trigger for the DAG it represents.

Upon executing the combined DAG, the user is presented with a combined set of input parameters for all DAGs, minus: those which will be dynamically generated and passed from one to the next (e.g. output data name and location), and those values which apply to multiple DAGs and are repetitions. This logic is controlled through a heirarchy of classes defined in `params.py`.