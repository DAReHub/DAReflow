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
    dag_id='floodEvent_MATSim',
    description='Run floodEvent module and MATSim as a chain',
    default_args=default_args,
    schedule=None,
    catchup=False,
    params=parameters.default_params() | parameters.floodEvent_matsim(),
    render_template_as_native_obj=True,
    tags=["chain", "floodEvent", "MATSim"],
) as dag:

    run_floodEvent = TriggerDagRunOperator(
        task_id='run_floodEvent',
        trigger_dag_id='floodEvent',
        conf="{{ params }}",
        execution_date="{{ execution_date }}",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=['success'],
    )

    run_matsim = TriggerDagRunOperator(
        task_id='run_matsim',
        trigger_dag_id='MATSim',
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
        op_kwargs={"dag_ids": ["floodEvent", "MATSim"]}
    )

    run_floodEvent >> run_matsim
    run_floodEvent >> stop_dag
    run_matsim >> stop_dag