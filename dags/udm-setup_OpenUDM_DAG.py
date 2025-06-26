from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import scripts.utils.params as params
import scripts.utils.af_utils as af_utils


parameters = params.Parameters()

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='udm-setup_OpenUDM',
    description='Run udm-setup and OpenUDM as a chain',
    default_args=default_args,
    schedule=None,
    catchup=False,
    params=parameters.default_params() | parameters.udmSetup_OpenUDM(),
    render_template_as_native_obj=True,
    tags=["chain", "udm-setup", "UDM"],
) as dag:

    run_udmSetup = TriggerDagRunOperator(
        task_id='run_udmSetup',
        trigger_dag_id='udm-setup',
        conf="{{ params }}",
        execution_date="{{ execution_date }}",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
    )

    run_OpenUDM = TriggerDagRunOperator(
        task_id='run_OpenUDM',
        trigger_dag_id='OpenUDM',
        conf="{{ params }}",
        execution_date="{{ execution_date }}",
        wait_for_completion=True,
        poke_interval=10
    )

    stop_dag = PythonOperator(
        task_id='stop_dag',
        python_callable=af_utils.stop_all_dag_tasks,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,
        op_kwargs={"dag_ids": ["run_udmSetup", "run_OpenUDM"]}
    )

    dag_run_state = PythonOperator(
        task_id='dag_run_state',
        python_callable=af_utils.dag_run_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_udmSetup >> run_OpenUDM
    run_udmSetup >> stop_dag
    run_OpenUDM >> stop_dag >> dag_run_state