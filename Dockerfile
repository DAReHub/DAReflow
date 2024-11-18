FROM apache/airflow:2.10.2
COPY requirements.txt .
RUN pip install --default-timeout=100 -r requirements.txt
USER root
RUN apt-get update && apt-get install -y git
USER airflow