import xml.etree.ElementTree as ET
from airflow.models import Variable

def configure_config(filepath, file_tags):
    tree = ET.parse(filepath)
    root = tree.getroot()

    for tag_key, tag_defualt_val in file_tags.items():
        print(tag_key, tag_defualt_val)

        for p in root.findall(f".//param[@name='{tag_key}']"):
            p.set("value", str(tag_defualt_val))

    with open(filepath, 'w') as f:
        f.write('<?xml version="1.0" ?>\n')
        f.write(
            '<!DOCTYPE config SYSTEM "http://www.matsim.org/files/dtd/config_v2.dtd">\n'
        )

        tree.write(f, encoding="unicode")


def configure_main(params, input_osm_config, input_ptmapper_config):
    """
    Renames filenames provided within pt2matsim osm config with the respective
    filenames provided by the user
    """
    osm_file_tags = {
        "osmFile": "/data/in/osmFile.osm",
        "outputNetworkFile": "/data/out/MultiModalNetwork.xml",
        "outputCoordinateSystem": "EPSG:" + params['PT2MATSIM_datavalue_crs']
    }
    ptm_file_tags = {
        "inputNetworkFile": "/data/out/MultiModalNetwork.xml",
        "inputScheduleFile": "/data/out/transitSchedule.xml",
        "outputNetworkFile": "/data/out/network.xml",
        "outputScheduleFile": "/data/out/scheduleFile.xml",
        "outputStreetNetworkFile": "/data/out/streetNetworkFile.xml",
        "numOfThreads": Variable.get("PT2MATSIM_THREADS"),
    }
    configure_config(input_osm_config, osm_file_tags)
    configure_config(input_ptmapper_config, ptm_file_tags)