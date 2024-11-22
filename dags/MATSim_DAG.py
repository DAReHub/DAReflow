from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount
import os
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

import scripts.utils.postgres_utils as postgres_utils
import scripts.utils.minio_utils as minio_utils
import scripts.utils.params as params
import scripts.utils.af_utils as af_utils
import scripts.utils.docker_utils as docker_utils
import scripts.utils.matsim_utils as matsim_utils


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
    max_active_runs=int(Variable.get("MATSIM_MAX_ACTIVE_RUNS")),
    schedule=None,
    params=parameters.default_params() | parameters.matsim(),
    render_template_as_native_obj=True,
    tags=["MATSim", "processor"]
) as dag:

    start_date = "{{ data_interval_start.strftime('%Y-%m-%d_%H-%M-%S') }}.{{ '{:03d}'.format(data_interval_start.microsecond // 1000) }}"
    scenario = "{{ dag_run.conf['scenario_name'] }}"

    airflow_input_run = os.getenv("AIRFLOW_MATSIM_INPUT") + start_date
    airflow_output_run = os.getenv("AIRFLOW_MATSIM_OUTPUT") + start_date
    output_path = f"MATSim/{scenario}/{start_date}"
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
            "data_interval_start": start_date
        }
    )

    configure_config = PythonOperator(
        task_id='configure_config',
        python_callable=matsim_utils.configure_config,
        op_kwargs={
            "params": "{{ dag_run.conf }}",
            "input_dir": airflow_input_run
        }
    )

    prepare_image = PythonOperator(
        task_id='prepare_image',
        python_callable=docker_utils.prep_image,
        op_kwargs={
            "params": "{{ dag_run.conf }}",
        }
    )

    periodic_post = PythonOperator(
        task_id='periodic_post',
        python_callable=af_utils.periodic_copier,
        provide_context=True,
        dag=dag,
        op_kwargs={
            "bucket": output_bucket,
            "src": airflow_output_run,
            "dst": output_path,
            "monitored_task": "run_matsim"
        }
    )

    run_matsim = CustomDockerOperator(
        api_version='auto',
        task_id='run_matsim',
        image="{{ dag_run.conf['MATSim_selection_image'] }}",
        container_name='airflow-matsim_' + start_date,
        # cpus=0.1,
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

    post_final_data = PythonOperator(
        task_id='post_final_data',
        python_callable=minio_utils.post_outputs,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
        op_kwargs={
            "bucket": output_bucket,
            "src": airflow_output_run,
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
        }
    )

    dag_run_state = PythonOperator(
        task_id='dag_run_state',
        python_callable=af_utils.dag_run_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
        op_kwargs={
            "excluded_tasks": ["periodic_post"]
        }
    )

    # SEQUENCE
    record_run_start >> setup_environment >> stage_data >> configure_config >> prepare_image >> run_matsim >> post_final_data >> record_run_end >> dag_run_state
    prepare_image >> periodic_post >> post_final_data