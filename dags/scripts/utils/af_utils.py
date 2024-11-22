from airflow.models.log import Log
from airflow.utils.db import create_session
from airflow.datasets.metadata import Metadata
from airflow.exceptions import AirflowSkipException
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.utils.state import State
from airflow.models import TaskInstance
from airflow.utils.db import provide_session
from airflow.models import Variable
import time
import scripts.utils.minio_utils as minio_utils


# Buggy - this gets the last user who ran the DAG. This is fine if you have just
# triggered the same DAG, but if it is a chained dag and someone else last ran
# it, you will get the other persons username. There doesn't appear to be an
# easy way around this in airflow - all 'user' functionality has been removed.
def get_user(dag_id):
    print("Getting username")
    with create_session() as session:
        return (
            session.query(Log.owner)
            .filter(Log.dag_id == dag_id, Log.event == "trigger")
            .order_by(Log.dttm.desc())
            .limit(1)
            .scalar()
        )


def stop_all_dag_tasks(dag_ids, **kwargs):
    for dag in dag_ids:

        try:
            dag_run = DagRun.find(
                dag_id=dag,
                execution_date=kwargs["logical_date"],
            )[0]

            for task_instance in dag_run.get_task_instances():
                if task_instance.current_state() in (
                    State.RUNNING,  # there may be more states to check ¯\_(ツ)_/¯
                    State.SCHEDULED,
                    State.UP_FOR_RESCHEDULE,
                ):
                    task_instance.set_state(State.FAILED)
        except:
            print(f"No dag_id named {dag} found")
            continue


def periodic_copier(bucket, src, dst, monitored_task, **context):
    interval = int(Variable.get("PERIODIC_COPY_INTERVAL"))
    print(f"Outputs will be uploaded to the database every {interval} seconds "
          f"(not accounting for upload durations). Please check output "
          f"timestamps to ensure you are working with an up to date copy.")

    started = False

    while True:
        time.sleep(5)
        all_task_states = get_all_tasks_status(context)
        task = all_task_states[monitored_task]

        if task is None:
            continue

        elif task == "running":
            if started is False:
                start_time = int(time.time())
                started = True
                continue

            current_time = int(time.time())
            if (current_time - start_time) < interval:
                continue

            print("UPLOADING")
            try:
                minio_utils.post_outputs(bucket, src, dst)
            except Exception as e:
                print("UPLOAD FAILED -> ", e)

            start_time = int(time.time())
            print("SLEEPING")

        else:
            break


def dag_run_status(excluded_tasks=None, **context):
    states = get_all_tasks_status(context)
    # Apply test to all tasks except for this one and excluded_tasks
    states.pop("dag_run_state")
    if excluded_tasks is not None:
        for task in excluded_tasks:
            states.pop(task)
    print("states", states)
    failed = {key: value for key, value in states.items() if value != "success"}
    if failed:
        raise AirflowException


def check_upstream_state(task_id, context):
    ti = context['ti']

    dag_id = ti.dag_id
    execution_date = ti.execution_date

    @provide_session
    def get_task_state(session=None):
        upstream_ti = session.query(TaskInstance).filter(
            TaskInstance.task_id == task_id,
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == execution_date
        ).first()
        return upstream_ti.state if upstream_ti else None

    return get_task_state()


def get_upstream_tasks_status(context):
    # Get the current task instance and DAG context
    ti = context['ti']
    dag_id = ti.dag_id
    execution_date = ti.execution_date
    task_id = ti.task_id
    upstream_tasks_status = {}

    @provide_session
    def fetch_upstream_states(session=None):
        # Get the DAG run for the current execution date
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == execution_date
        ).first()

        # Find all upstream tasks of the current task
        upstream_tasks = ti.task.upstream_list

        for upstream_task in upstream_tasks:
            upstream_ti = session.query(TaskInstance).filter(
                TaskInstance.task_id == upstream_task.task_id,
                TaskInstance.dag_id == dag_id,
                TaskInstance.execution_date == execution_date
            ).first()
            if upstream_ti:
                upstream_tasks_status[upstream_task.task_id] = upstream_ti.state

    fetch_upstream_states()

    return upstream_tasks_status


def get_all_tasks_status(context):
    # Get the current task instance and DAG context
    ti = context['ti']
    dag_id = ti.dag_id
    execution_date = ti.execution_date
    all_tasks_status = {}

    @provide_session
    def fetch_all_task_states(session=None):
        # Get the DAG run for the current execution date
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date == execution_date
        ).first()

        # Get all task instances for the DAG run
        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.execution_date == execution_date
        ).all()

        # Populate the dictionary with task IDs and their states
        for task_instance in task_instances:
            all_tasks_status[task_instance.task_id] = task_instance.state

    fetch_all_task_states()

    return all_tasks_status


# Selects which set of params to use based on run type, as default manual params are always present even if dataset triggered
def dataset_initialise_params(dag_id=None, dataset=None, *, inlet_events, **context):
    print("Initialising parameters")
    trigger_type = context["run_id"].split("__")[0]
    print("trigger type:", trigger_type)
    if trigger_type == "manual":
        params = context["params"]
    elif trigger_type == "dataset_triggered":
        events = inlet_events[dataset]
        params = events[-1].extra
        # Check if dicts are nested (i.e. chained). When chaining, param keys and datasets must match dag_ids
        if isinstance(params[0], dict):
            params = params[dag_id]
    else:
        raise Exception
    print("params:", params)
    return params


def dataset_yield_params(dataset, **context):
    params = context["params"]
    if "chain" in params.keys() and params["chain"] is True:
        yield Metadata(dataset, context["params"])
    else:
        raise AirflowSkipException


def dataset_parseParams(params):
    # new_params = {}
    # for module_key, module_value in params.items():
    #     new_params[module_key] = {}
    #     for param_key, param_value in module_value.items():
    #         new_params[module_key][param_key] = param_value["__data__"]["value"]
    # return new_params
    print("params", params)
    new_params = {}
    for key, value in params.items():
        print(key, value)
        module_name = key.split("_")[0]
        module_param = key.replace(module_name + "_", "")
        if module_name not in new_params:
            new_params[module_name] = {}
        new_params[module_name][module_param] = value["__data__"]["value"]
    print("new_params", new_params)
    return new_params