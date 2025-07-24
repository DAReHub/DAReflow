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


class CustomDockerOperator(DockerOperator):
    template_fields = DockerOperator.template_fields + ('mounts',)

with DAG(
    dag_id='udmsetup',
    description='Setup data for UDM model.',
    default_args={'owner': 'airflow'},
    max_active_runs=int(Variable.get("UDMSETUP_MAX_ACTIVE_RUNS")),
    schedule=None,
    params=vars(params.UdmSetup()),
    render_template_as_native_obj=True,
    tags=["UDM", "processor"]
) as dag:

    start_date = "{{ data_interval_start.strftime('%Y-%m-%d_%H-%M-%S') }}.{{ '{:03d}'.format(data_interval_start.microsecond // 1000) }}"
    scenario = "{{ dag_run.conf['scenario_name'] }}"

    airflow_input_run = os.getenv("AIRFLOW_UDMSETUP_INPUT") + start_date
    airflow_output_run = os.getenv("AIRFLOW_UDMSETUP_OUTPUT") + start_date
    output_path = f"udmsetup/{scenario}/{start_date}"
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

    prepare_image = PythonOperator(
        task_id='prepare_image',
        python_callable=docker_utils.prep_image,
        provide_context=True,
        op_kwargs={
            "params": "{{ dag_run.conf }}",
        }
    )

    run_udmSetup = CustomDockerOperator(
        api_version='auto',
        task_id='run_udmSetup',
        image="{{ dag_run.conf['udmsetup_selection_image'] }}",
        container_name='airflow-udmsetup_' + start_date,
        # cpus=0.1,
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='bridge',
        dag=dag,
        # Target mounts are within the called upon docker container, not the
        # airflow container or on host.
        mounts=[
            Mount(
                source=os.getenv("HOST_UDMSETUP_INPUT") + start_date,
                target=os.getenv("UDMSETUP_CONTAINER_INPUT"),
                type='bind',
                read_only=True
            ),
            Mount(
                source=os.getenv("HOST_UDMSETUP_OUTPUT") + start_date,
                target=os.getenv("UDMSETUP_CONTAINER_OUTPUT"),
                type='bind'
            ),
        ],
        mount_tmp_dir=False,
        environment={
            'UDMSETUP_OUTPUT_OVERWRITE': 'true',
            'attractors': "{{ dag_run.conf['udmsetup_datavalue_attractors'] }}",
            'constraints': "{{ dag_run.conf['udmsetup_datavalue_constraints'] }}",
            'current_development': "{{ dag_run.conf['udmsetup_datavalue_current_development'] }}",
            'density_from_raster': "{{ dag_run.conf['udmsetup_datavalue_density_from_raster'] }}",
            'people_per_dwelling': "{{ dag_run.conf['udmsetup_datavalue_people_per_dwelling'] }}",
            'coverage_threshold': "{{ dag_run.conf['udmsetup_datavalue_coverage_threshold'] }}",
            'minimum_development_area': "{{ dag_run.conf['udmsetup_datavalue_minimum_development_area'] }}",
            'maximum_plot_size': "{{ dag_run.conf['udmsetup_datavalue_maximum_plot_size'] }}",
            'OUTPUT_TITLE': 'airflow_run',
            'OUTPUT_DESCRIPTION': 'airflow_run'
            #'extra_parameters': "{{ dag_run.conf['udmsetup_datavalue_extra_parameters'] }}"
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
    )

    # SEQUENCE
    record_run_start >> setup_environment >> stage_data >> prepare_image >> run_udmSetup >> post_final_data >> record_run_end >> dag_run_state