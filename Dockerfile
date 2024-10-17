FROM apache/airflow:2.10.2
COPY requirements.txt .
RUN pip install --default-timeout=100 -r requirements.txt