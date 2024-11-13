import xml.etree.ElementTree as ET
from pathlib import Path
import os

def configure_config(params, input_dir):
    filepath = input_dir + '/config.xml'

    # Must match matsim params
    file_tags = {
        "inputNetworkFile": "null",
        "inputPlansFile": "null",
        "inputCountsFile": "null",
        "vehiclesFile": "null",
        "inputChangeEventsFile": "null",
        "transitScheduleFile": "null",
        "transitLinesAttributesFile": "null",
        "transitStopsAttributesFile": "null",
        "laneDefinitionsFile": "null",
        "inputPersonAttributesFile": "null",
        "inputAlightCountsFile": "null",
        "inputBoardCountsFile": "null",
        "inputOccupancyCountsFile": "null",
    }

    sys_tags = {
        "numberOfThreads": os.getenv("MATSIM_THREADS"),
        "outputDirectory": "./output"
    }

    tree = ET.parse(filepath)
    root = tree.getroot()

    for tag_key, tag_defualt_val in file_tags.items():
        print(tag_key, tag_defualt_val)

        param_val = params["MATSim_datapath_" + tag_key]
        print(param_val)

        if param_val is None:
            continue

        else:
            suffix = Path(param_val).suffixes
            print(suffix)

            for p in root.findall(f".//param[@name='{tag_key}']"):
                p.set("value", tag_key + "".join(suffix))

    for tag_key, tag_defualt_val in sys_tags.items():
        for item in root.findall(f".//param[@name='{tag_key}']"):
            item.set("value", str(tag_defualt_val))

    with open(filepath, 'w') as f:
        f.write('<?xml version="1.0" ?>\n')
        f.write(
            '<!DOCTYPE config SYSTEM "http://www.matsim.org/files/dtd/config_v2.dtd">\n'
        )

        tree.write(f, encoding="unicode")