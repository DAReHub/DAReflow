from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount
import os

import scripts.utils.postgres_utils as postgres_utils
import scripts.utils.minio_utils as minio_utils
import scripts.utils.params as params
import scripts.utils.af_utils as af_utils


class CustomDockerOperator(DockerOperator):
    template_fields = DockerOperator.template_fields + ('mounts',)


parameters = params.Parameters()

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='MATSim',
    description='Run MATSim',
    default_args=default_args,
    max_active_runs=1,
    schedule=None,
    params=parameters.default_params() | parameters.matsim(),
    render_template_as_native_obj=True,
    tags=["MATSim", "processor"]
) as dag:

    start_date = "{{ dag_run.start_date.strftime('%Y-%m-%d_%H-%M-%S') }}.{{ '{:03d}'.format(dag_run.start_date.microsecond // 1000) }}"
    user = af_utils.get_user('MATSim')
    scenario = "{{ dag_run.conf['scenario_name'] }}"

    airflow_input_run = os.getenv("AIRFLOW_MATSIM_INPUT") + start_date
    airflow_output_run = os.getenv("AIRFLOW_MATSIM_OUTPUT") + start_date
    output_path = f"MATSim/{user}/{scenario}/{start_date}"
    output_bucket = os.getenv("DEFUALT_OUTPUT_BUCKET")

    # TASKS
    record_run_start = PythonOperator(
        task_id='record_run_start',
        python_callable=postgres_utils.update_runs,
        provide_context=True,
        op_kwargs={
            "dag_stage": "start",
            "params": "{{ dag_run.conf }}",
            "output_path": output_path,
        }
    )

    setup_environment = BashOperator(
        task_id='setup_environment',
        bash_command=f'mkdir {airflow_input_run} {airflow_output_run}'
    )

    stage_data = PythonOperator(
        task_id='stage_data',
        python_callable=minio_utils.get_inputs,
        provide_context=True,
        dag=dag,
        op_kwargs={
            "params": "{{ dag_run.conf }}",
            "dst": airflow_input_run,
        }
    )

    run_matsim = CustomDockerOperator(
        api_version='auto',
        task_id='run_matsim',
        image="{{ dag_run.conf['MATSim_selection_image'] }}",
        container_name='airflow-matsim_' + start_date,
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='bridge',
        dag=dag,
        # Target mounts are within the called upon docker container, not the
        # airflow container or on host.
        mounts=[
            Mount(
                source=os.getenv("HOST_MATSIM_INPUT") + start_date,
                target=os.getenv("MATSIM_CONTAINER_INPUT"),
                type='bind',
                read_only=True
            ),
            Mount(
                source=os.getenv("HOST_MATSIM_OUTPUT") + start_date,
                target=os.getenv("MATSIM_CONTAINER_OUTPUT"),
                type='bind'
            ),
        ],
        mount_tmp_dir=False,
        environment={
            'MATSIM_OUTPUT_OVERWRITE': 'true'
        },
    )

    post_data = PythonOperator(
        task_id='post_data',
        python_callable=minio_utils.upload_data,
        provide_context=True,
        dag=dag,
        op_kwargs={
            "bucket": output_bucket,
            "src": airflow_output_run,
            "dst": output_path,
        }
    )

    record_run_end = PythonOperator(
        task_id='record_run_end',
        python_callable=postgres_utils.update_runs,
        provide_context=True,
        # trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={
            "dag_stage": "end",
            "params": "{{ dag_run.conf }}",
            "output_path": output_path,
        }
    )

    # SEQUENCE
    record_run_start >> setup_environment >> stage_data >> run_matsim >> post_data >> record_run_end