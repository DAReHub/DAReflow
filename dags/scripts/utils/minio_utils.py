import os
from minio import Minio
from minio.error import S3Error
from pathlib import Path
from datetime import datetime

import scripts.utils.af_utils as af_utils

# TODO: Try implementing a hook to minio instead
def minio_client():
    return Minio(
        os.getenv("AIRFLOW_VAR_MINIO_ADDRESS"),
        access_key=os.getenv("AIRFLOW_VAR_MINIO_USER"),
        secret_key=os.getenv("AIRFLOW_VAR_MINIO_PASSWORD"),
        secure=False  # TODO: Set to True if you're using HTTPS
    )


def import_file(bucket: str, src: str, dst: str):
    client = minio_client()
    try:
        client.fget_object(bucket, src, dst)
        print("Successfully loaded:", src)
    except Exception as e:
        print("Failed to load: ", src, e)


# Iterates over files in a directory to find all with a prefix, and returns the first
def search_prefix_file(bucket, dir, prefix):
    client = minio_client()
    objects = client.list_objects(bucket, prefix=dir, recursive=False)
    object_names = [obj.object_name for obj in objects]
    return [item for item in object_names if item.split("/")[-1].startswith(prefix)]


def search_prefix_dir(bucket, dir, prefix):
    client = minio_client()
    objects = client.list_objects(bucket, prefix=dir, recursive=False)
    object_names = [obj.object_name for obj in objects]
    # [-2] here takes the directory name after the trailing slash
    return [item for item in object_names if item.split("/")[-2].startswith(prefix)]


def upload_data(bucket: str, src: str, dst: str):
    client = minio_client()

    for root, dirs, files in os.walk(src):
        for file in files:
            local_file = os.path.join(root, file)
            relative_path = os.path.relpath(local_file, src)
            minio_file = os.path.join(dst, relative_path)

            client.fput_object(
                bucket_name=bucket,
                object_name=minio_file,
                file_path=local_file,
            )
            print(f"Uploaded {local_file} to {minio_file} in bucket {bucket}")


def get_input_datadir(bucket, src, dst, param_key):
    client = minio_client()

    objects = client.list_objects(bucket, prefix=src, recursive=True)

    for obj in objects:
        object_filename = obj.object_name.split("/")[-1]

        dir_name = param_key.replace("datadir_", "")
        src_path = src + object_filename
        dst_path = dst + '/' + dir_name + '/' + object_filename
        print('src:', src_path)
        print('dst:', dst_path)

        try:
            client.fget_object(bucket, src_path, dst_path)
            print(f"Successfully copied {object_filename} to {dst_path}")
        except Exception as e:
            print(f"Failed to download {object_filename} from MinIO: {e}")


# def get_inputs(params, input_bucket, dst, **context):
#     dag_id = context["dag"].dag_id
#     print(dag_id)
#     scenario_name = params["scenario_name"]
#     scenario_prefix = params["scenario_prefix"]
#
#     def download_file(param_key, param_path):
#         if scenario_prefix:
#             try:
#                 paths_to_download = search_prefix_file(
#                     input_bucket, param_path, scenario_name + '_'
#                 )
#             except Exception as e:
#                 print(f"Could not fetch files with prefix '{scenario_name}_' in '{param_path}': ", e)
#         else:
#             paths_to_download = [param_path]
#
#         for path in paths_to_download:
#             filename = param_key.replace("datapath_", "")
#             dst_filename = filename + "".join(Path(path).suffixes)
#             dst_path = dst + '/' + dst_filename
#             import_file(input_bucket, path, dst_path)
#
#     def download_directory(param_key, param_path):
#         if not param_path.endswith("/"):
#             param_path += '/'
#
#         if scenario_prefix:
#             try:
#                 directories = search_prefix_dir(
#                     input_bucket, param_path, scenario_name + '_'
#                 )
#                 print('directories', directories)
#             except Exception as e:
#                 print(f"Could not fetch directory with prefix '{scenario_name}_' in '{param_path}': ", e)
#             for directory in directories:
#                 get_input_datadir(
#                     input_bucket,
#                     directory,
#                     dst,
#                     param_key
#                 )
#         else:
#             get_input_datadir(
#                 input_bucket,
#                 param_path,
#                 dst,
#                 param_key
#             )
#
#     for param_key, param_path in params.items():
#         print('param_key:', param_key, 'param_path:', param_path)
#         if not param_key.startswith(dag_id):
#             continue
#         elif param_path is None:
#             continue
#
#         param_key = param_key.replace(dag_id + "_", "")
#
#         if "<run_name>" in param_path:
#             print("<run_name>", param_path)
#             param_path = param_path.replace(
#                 "<run_name>",
#                 scenario_name + "_" + context["run_id"]
#             )
#             print("new:", param_path)
#
#         if param_key.startswith("datapath_"):
#             download_file(param_key, param_path)
#         elif param_key.startswith("datadir_"):
#             download_directory(param_key, param_path)


def get_inputs(params, dst, **context):
    dag_id = context["dag"].dag_id
    scenario_name = params["scenario_name"]

    for param_key, param_path in params.items():
        print('param_key:', param_key, 'param_path:', param_path)
        if not param_key.startswith(dag_id):
            continue
        elif param_path is None:
            print(f"{param_key} not specified: skipping")
            continue

        param_key = param_key.replace(dag_id + "_", "")

        if "<user>/<scenario>/<start_date>" in param_path:
            param_path = param_path.replace("<user>", af_utils.get_user(dag_id))
            param_path = param_path.replace("<scenario>", scenario_name)

            start_date = context['dag_run'].start_date
            start_date = start_date.strftime("%Y-%m-%d_%H-%M-%S") + f".{start_date.microsecond // 1000:03d}"
            param_path = param_path.replace("<start_date>", start_date)

            print("new:", param_path)


        if param_key.startswith("datapath_"):
            input_bucket = param_path.split("/")[0]
            param_path = param_path.replace(input_bucket + '/', "")
            filename = param_key.replace("datapath_", "")
            dst_filename = filename + "".join(Path(param_path).suffixes)
            dst_path = dst + '/' + dst_filename
            import_file(input_bucket, param_path, dst_path)

        elif param_key.startswith("datadir_"):
            if not param_path.endswith("/"):
                param_path += '/'
            input_bucket = param_path.split("/")[0]
            param_path = param_path.replace(input_bucket + '/', "")
            get_input_datadir(input_bucket, param_path, dst, param_key)