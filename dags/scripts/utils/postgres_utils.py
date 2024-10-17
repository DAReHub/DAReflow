import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
import os
import json

import scripts.utils.af_utils as af_utils
import scripts.utils.misc_utils as misc_utils


def open_connection():
    pg_hook = PostgresHook(postgres_conn_id=os.getenv("AIRFLOW_VAR_POSTGRES_URL"))
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    return pg_hook, conn, cursor


def close_connection(conn, cursor):
    cursor.close()
    conn.close()


def update_runs(dag_stage, params, output_path=None, **context):
    pg_hook, conn, cursor = open_connection()

    image_key = misc_utils.find_key_containing_string("selection_image", params)

    cursor.execute(
        f"INSERT INTO runs (dag_stage, state, dag_id, run_id, params, scenario,  image, username, dag_start_time, trigger_type, output_directory) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            dag_stage,
            json.dumps(af_utils.get_all_tasks_status(context)),
            context["dag"].dag_id,
            context["run_id"],
            json.dumps(params),
            params["scenario_name"],
            params[image_key] if image_key is not None else None,
            af_utils.get_user(context["dag"].dag_id),
            context['dag_run'].start_date,
            context["run_id"].split("__")[0].split("_")[-1],
            output_path
        )
    )

    conn.commit()
    close_connection(conn, cursor)