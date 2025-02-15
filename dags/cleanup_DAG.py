# !!! If airflow experiences issues with delete permissions, on the host system
# run:
#   sudo setfacl -R -m u:id_no:rwx /path/to/staging/dir
#   sudo setfacl -d -m u:id_no:rwx /path/to/staging/dir
# where id_no is airflow host user id found by:
#   docker exec -it <container_name> id
# These should ensure airflow has permission to delete current and future items
# only within the staging directory.

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import scripts.utils.os_utils as os_utils

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='cleanup',
    default_args=default_args,
    description='A daily DAG to delete cached data older than 7 days',
    schedule_interval=timedelta(days=1),
    catchup=True,
    tags=["maintenance"]
)

clean_cache = PythonOperator(
    task_id='clean_cache',
    python_callable=os_utils.remove_cached_data,
    dag=dag,
    op_kwargs={
        "base_dir": "/opt/airflow/cache/",
        "days_to_keep": int(Variable.get("STAGED_DATA_RETENTION"))
    }
)

clean_cache
