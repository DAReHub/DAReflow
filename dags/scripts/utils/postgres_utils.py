import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
import os
import json

import scripts.utils.af_utils as af_utils
import scripts.utils.misc_utils as misc_utils


class PostgresConnectionHook:
    def __init__(self, connection_name):
        self.hook = PostgresHook(postgres_conn_id=connection_name)
        self.conn = self.hook.get_conn()
        self.cursor = self.conn.cursor()

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

    def update_runs(self, dag_stage, params, output_path, context):
        image_key = misc_utils.find_key_containing_string("selection_image", params)
        self.cursor.execute(
            f"INSERT INTO airflow_runs (dag_id, dag_stage, run_start_time, run_id, run_trigger_type, task_states, context_params, scenario_name, docker_image, airflow_username, output_destination) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                context["dag"].dag_id,
                dag_stage,
                context['dag_run'].start_date,
                context["run_id"],
                context["run_id"].split("__")[0].split("_")[-1],
                json.dumps(af_utils.get_all_tasks_status(context)),
                json.dumps(params),
                params["scenario_name"],
                params[image_key] if image_key is not None else None,
                af_utils.get_user(context["dag"].dag_id),
                output_path
            )
        )
        self.conn.commit()


def submit_metadata(dag_stage, params, output_path=None, **context):
    db = PostgresConnectionHook(os.getenv("POSTGRES_AIRFLOW_CONN_ID"))
    db.update_runs(dag_stage, params, output_path, context)
    db.close_connection()