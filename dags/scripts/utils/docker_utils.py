import os

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


# Downloads and loads image if not on the system already
def prep_image(params):
    image_key = misc_utils.find_key_containing_string("selection_image", params)
    image_name = params[image_key]
    images = list_system_images(image_name.split(":")[0])
    if image_name in images:
        print("Image already available")
    else:
        print("Fetching image")
        dst = os.getenv("AIRFLOW_IMAGES_INPUT")
        minio_utils.import_file(
            bucket=os.getenv("DOCKER_IMAGES_BUCKET"),
            src="MATSim/" + image_name + ".tar",
            dst=dst + image_name + ".tar"
        )
        print("Loading image")
        os.system(f"export DOCKER_HOST={docker_proxy} && docker load -i {dst}{image_name}.tar")
        print("Cleaning up")
        os.remove(dst + image_name + ".tar")
        print("Done")