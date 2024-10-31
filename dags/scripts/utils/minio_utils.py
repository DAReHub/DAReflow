import os
from minio import Minio
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pathlib import Path

import scripts.utils.af_utils as af_utils


class MinioS3Hook:
    def __init__(self, connection_name):
        self.connection = S3Hook(aws_conn_id=connection_name)

    def get_file(self, bucket, src, dst):
        key = self.connection.get_key(key=src, bucket_name=bucket)

        if key:
            key.download_file(dst)
            print(f"Downloaded {src} from bucket {bucket} to {dst}")
        else:
            print(f"Object {src} not found in bucket {bucket}")

    def get_directory_files(self, bucket, src, dst, param_key):
        objects = self.connection.list_keys(bucket_name=bucket, prefix=src)

        for obj in objects:
            object_filename = obj.split("/")[-1]

            dir_name = param_key.replace("datadir_", "")
            dst_path = dst + '/' + dir_name + '/' + object_filename
            print('src:', obj)
            print('dst:', dst_path)

            self.get_file(bucket, obj, dst_path)

    def post_data(self, bucket, src, dst):
        for root, dirs, files in os.walk(src):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, src)
                minio_file = os.path.join(dst, relative_path)

                print("local_file: ", local_file)
                print("bucket: ", bucket)
                print("relative_path: ", relative_path)
                print("minio_file: ", minio_file)

                self.connection.load_file(
                    filename=local_file,
                    bucket_name=bucket,
                    key=minio_file,
                    replace=True
                )

                print(f"Uploaded {local_file} to {minio_file} in bucket {bucket}")


class MinioPython:
    def __init__(self):
        self.connection = Minio(
            os.getenv("AIRFLOW_VAR_MINIO_ADDRESS"),
            access_key=os.getenv("AIRFLOW_VAR_MINIO_USER"),
            secret_key=os.getenv("AIRFLOW_VAR_MINIO_PASSWORD"),
            secure=True
        )

    def get_file(self, bucket, src, dst):
        try:
            self.connection.fget_object(bucket, src, dst)
            print("Successfully loaded:", src)
        except Exception as e:
            print("Failed to load: ", src, e)

    def get_directory_files(self, bucket, src, dst, param_key):
        objects = self.connection.list_objects(bucket, prefix=src, recursive=True)

        for obj in objects:
            object_filename = obj.object_name.split("/")[-1]

            dir_name = param_key.replace("datadir_", "")
            src_path = src + object_filename
            dst_path = dst + '/' + dir_name + '/' + object_filename
            print('src:', src_path)
            print('dst:', dst_path)

            self.get_file(bucket, src, dst)

    def post_data(self, bucket, src, dst):
        for root, dirs, files in os.walk(src):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, src)
                minio_file = os.path.join(dst, relative_path)

                self.connection.fput_object(
                    bucket_name=bucket,
                    object_name=minio_file,
                    file_path=local_file,
                )
                print(f"Uploaded {local_file} to {minio_file} in bucket {bucket}")


def get_inputs(params, dst, **context):
    conn = MinioS3Hook(os.getenv("MINIO_AIRFLOW_CONN_ID"))

    dag_id = context["dag"].dag_id
    scenario_name = params["scenario_name"]

    # A param key (defined in utils/params.py) consists of a model name, path
    #   type, and target filename, all of which are separated by an underscore.
    # Iterate through all config params and decide what to do for each based on
    #   their key.
    for param_key, param_path in params.items():
        print('param_key:', param_key, 'param_path:', param_path)

        # Don't deal with params not for the current dag or left empty
        if not param_key.startswith(dag_id):
            continue
        elif param_path is None:
            print(f"{param_key} not specified: skipping")
            continue

        # Remove key prefix relating to dag
        param_key = param_key.replace(dag_id + "_", "")

        # Get the path to a coupled model output
        if "<user>/<scenario>/<start_date>" in param_path:
            param_path = param_path.replace("<user>", af_utils.get_user(dag_id))
            param_path = param_path.replace("<scenario>", scenario_name)

            start_date = context['dag_run'].start_date
            start_date = start_date.strftime("%Y-%m-%d_%H-%M-%S") + f".{start_date.microsecond // 1000:03d}"
            param_path = param_path.replace("<start_date>", start_date)

            print("new:", param_path)

        # Download a single file from a filepath
        if param_key.startswith("datapath_"):
            input_bucket = param_path.split("/")[0]
            param_path = param_path.replace(input_bucket + '/', "")
            filename = param_key.replace("datapath_", "")
            dst_filename = filename + "".join(Path(param_path).suffixes)
            dst_path = dst + '/' + dst_filename
            conn.get_file(input_bucket, param_path, dst_path)

        # Download all files within a directory
        elif param_key.startswith("datadir_"):
            if not param_path.endswith("/"):
                param_path += '/'
            input_bucket = param_path.split("/")[0]
            param_path = param_path.replace(input_bucket + '/', "")
            conn.get_directory_files(input_bucket, param_path, dst, param_key)


def post_outputs(bucket, src, dst):
    conn = MinioS3Hook(os.getenv("MINIO_AIRFLOW_CONN_ID"))
    conn.post_data(bucket, src, dst)