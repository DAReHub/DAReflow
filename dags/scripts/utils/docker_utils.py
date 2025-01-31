import os
from airflow.exceptions import AirflowException
import scripts.utils.minio_utils as minio_utils
import scripts.utils.misc_utils as misc_utils

docker_proxy = os.getenv("DOCKER_PROXY")

# Lists all images and tags on the system with the provided image name
def list_system_images(image_name):
    results = os.popen(f'export DOCKER_HOST={docker_proxy} && docker images {image_name}').read()
    results = results.split("\n")
    results = [r.split(" ") for r in results]
    results = [list(filter(None, r)) for r in results]
    names = []
    for r in results:
        if 'REPOSITORY' in r or r == []:
            continue
        names.append(r[0] + ":" + r[1])
    print("Images found:", names)
    return names


def get_param_key(params, dag_id):
    for param_key, param_path in params.items():
        if not param_key.startswith(dag_id):
            continue
        if not param_key.endswith("selection_image"):
            continue
        return param_key


# Downloads and loads image if not on the system already
def prep_image(params, param_key=None, **context):
    dag_id = context["dag"].dag_id

    if param_key is None:
        param_key = get_param_key(params, dag_id)

    if not param_key.endswith("selection_image"):
        param_key = param_key + "_selection_image"

    image_name = params[param_key]

    if image_name.endswith(".tar"):
        image_name = image_name.replace(".tar", "")
    images = list_system_images(image_name.split(":")[0])

    if image_name in images:
        print("Image already available")

    else:
        try:
            print(f"Attepting to fetch image '{image_name}' from DB")
            dst = os.getenv("AIRFLOW_IMAGES_INPUT")
            conn = minio_utils.MinioS3Hook(os.getenv("MINIO_AIRFLOW_CONN_ID"))
            conn.get_file(
                bucket=os.getenv("DOCKER_IMAGES_BUCKET"),
                src=context["dag"].dag_id + "/" + image_name + ".tar",
                dst=dst + image_name + ".tar"
            )
            print("Loading image")
            os.system(f"export DOCKER_HOST={docker_proxy} && docker load -i {dst}{image_name}.tar")
            print("Cleaning up")
            os.remove(dst + image_name + ".tar")
            print("Done")

        except Exception as e:
            print(f"Failed to fetch image '{image_name}' from DB - Ensure image directory name in DB matches DAG ID. ERROR: ", e)
            raise AirflowException