import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

import scripts.utils.af_utils as af_utils
import scripts.utils.minio_utils as minio_utils
import scripts.utils.postgres_utils as postgres_utils
import scripts.utils.params as params

sys.path.append("/opt/airflow/modules/" + Variable.get("FLOODEVENT_MODULE"))
import flood_network
import generate_changeEvents
import input_validation

parameters = params.Parameters()

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='floodEvent',
    description='Process a flooding event on the network',
    default_args=default_args,
    max_active_runs=int(Variable.get("FLOODEVENT_MAX_ACTIVE_RUNS")),
    schedule=None,
    params=parameters.default_params() | parameters.floodEvent(),
    render_template_as_native_obj=True,
    tags=["flood-event", "processor"]
) as dag:

    start_date = "{{ data_interval_start.strftime('%Y-%m-%d_%H-%M-%S') }}.{{ '{:03d}'.format(data_interval_start.microsecond // 1000) }}"
    scenario = "{{ dag_run.conf['scenario_name'] }}"

    # PATHS
    airflow_input_run = os.getenv("AIRFLOW_FLOODEVENT_INPUT") + start_date
    airflow_input_rasters = airflow_input_run + "/flood-rasters/"
    airflow_output_run = os.getenv("AIRFLOW_FLOODEVENT_OUTPUT") + start_date
    airflow_output_flood_network = airflow_output_run + "/flood_network/"
    airflow_output_networkChangeEvemts = airflow_output_run + "/networkChangeEvents/"
    output_path = f"floodEvent/{scenario}/{start_date}"
    output_bucket = os.getenv("DEFUALT_OUTPUT_BUCKET")

    # TASKS
    record_run_start = PythonOperator(
        task_id='record_run_start',
        python_callable=postgres_utils.submit_metadata,
        provide_context=True,
        op_kwargs={
            "dag_stage": "start",
            "params": "{{ dag_run.conf }}",
            "output_path": output_path,
            "module_name": Variable.get("FLOODEVENT_MODULE")
        }
    )

    setup_environment = BashOperator(
        task_id='setup_environment',
        bash_command=f'mkdir {airflow_input_run} {airflow_input_rasters} {airflow_output_run} {airflow_output_flood_network} {airflow_output_networkChangeEvemts}'
    )

    stage_data = PythonOperator(
        task_id='stage_data',
        python_callable=minio_utils.get_inputs,
        provide_context=True,
        dag=dag,
        op_kwargs={
            "params": "{{ dag_run.conf }}",
            "dst": airflow_input_run
        },
    )

    validate_inputs = PythonOperator(
        task_id='validate_inputs',
        python_callable=input_validation.main,
        provide_context=True,
        dag=dag,
        op_kwargs={"filepath": airflow_input_run}
    )

    flood_network = PythonOperator(
        task_id='flood_network',
        python_callable=flood_network.main,
        provide_context=True,
        op_kwargs={
            "config_filepath": airflow_input_run + '/config.json',
            "network_filepath": airflow_input_run + '/network.gpkg',
            "floodmap_dir": airflow_input_rasters,
            "output_dir": airflow_output_flood_network
        }
    )

    post_flood_network_data = PythonOperator(
        task_id='post_flood_network_data',
        python_callable=minio_utils.post_outputs,
        provide_context=True,
        dag=dag,
        op_kwargs={
            "bucket": output_bucket,
            "src": airflow_output_flood_network,
            "dst": output_path,
        }
    )

    generate_networkChangeEvents = PythonOperator(
        task_id='generate_networkChangeEvents',
        python_callable=generate_changeEvents.main,
        provide_context=True,
        op_kwargs={
            "config_filepath": airflow_input_run + '/config.json',
            "flood_network_csv_filepath": airflow_output_flood_network + 'flooded_network.csv',
            "output_dir": airflow_output_networkChangeEvemts
        }
    )

    post_changeEvents_data = PythonOperator(
        task_id='post_changeEvents_data',
        python_callable=minio_utils.post_outputs,
        provide_context=True,
        dag=dag,
        op_kwargs={
            "bucket": output_bucket,
            "src": airflow_output_networkChangeEvemts,
            "dst": output_path,
        }
    )

    record_run_end = PythonOperator(
        task_id='record_run_end',
        python_callable=postgres_utils.submit_metadata,
        provide_context=True,
        op_kwargs={
            "dag_stage": "end",
            "params": "{{ dag_run.conf }}",
            "output_path": output_path,
            "module_name": Variable.get("FLOODEVENT_MODULE")
        }
    )

    dag_run_state = PythonOperator(
        task_id='dag_run_state',
        python_callable=af_utils.dag_run_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # SEQUENCE
    record_run_start >> setup_environment >> stage_data >> validate_inputs >> flood_network >> generate_networkChangeEvents >> post_changeEvents_data >> record_run_end >> dag_run_state
    flood_network >> post_flood_network_data >> record_run_end