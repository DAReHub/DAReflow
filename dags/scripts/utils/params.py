import os
from airflow.models.param import Param


class Parameters:
    def __init__(self):
        pass

    def _modify_items(self, data_dict, exclude=None, changes=None):
        # Exclude expects list
        if exclude:
            for key in exclude:
                data_dict.pop(key, None)

        # Changes expect dict
        if changes:
            for key, value in changes.items():
                if key in data_dict:
                    data_dict[key] = value

        return data_dict

    def _render_params(self, data_dict):
        parameters = {}
        for key, value in data_dict.items():
            if key not in parameters:
                parameters[key] = {}
            parameters[key] = Param(
                default=value["default"],
                type=value["type"],
                title=value["title"],
                description=value["description"],
                # enum=value["enum"] if "enum" in value else []
            )
        return parameters

    def default_params(self, exclude=None, changes=None):
        data = {
            "scenario_name": Param(
                type="string",
                title="Scenario name",
                description="Name this scenario. Use the same scenario name as a previous run to save outputs within the same parent directory."
            )
        }
        return self._modify_items(data, exclude, changes)

    def matsim(self, exclude=None, changes=None):
        data = {
            "MATSim_selection_image": Param(
                "matsim:15.0-2022w40",
                type="string",
                title="[MATSim] Docker Image",
                description="Select a MATSim Docker image, as <name:tag>. See available images or upload your own (in .tar format) at docker-images bucket.",
            ),
            "MATSim_datapath_config": Param(
                type="string",
                title="[MATSim] config path",
                description=f"PATH to the config.xml file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_CONFIG_DEFAULT')}. !! Filepaths referenced within the config will be overwritten by any filepaths defined in this form. Config params which also define system performance (e.g. numberOfThreads) are also manipulated to suit the host system !!"
            ),
            "MATSim_datapath_inputNetworkFile": Param(
                type="string",
                title="[MATSim] inputNetworkFile path",
                description=f"PATH to the network .[xml, xml.gz] file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_NETWORK_DEFAULT')}"
            ),
            "MATSim_datapath_inputPlansFile": Param(
                type="string",
                title="[MATSim] inputPlansFile path",
                description=f"PATH to the plans .[xml, xml.gz] file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_PLANS_DEFAULT')}"
            ),
            "MATSim_datapath_inputCountsFile": Param(
                type=["string"],
                title="[MATSim] inputCountsFile path",
                description=f"PATH to the motor counts .xml file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_MOTORVEHCOUNTS_DEFAULT')}"
            ),
            "MATSim_datapath_vehiclesFile": Param(
                type=["null", "string"],
                title="[MATSim] transit vehiclesFile path"
            ),
            "MATSim_datapath_inputChangeEventsFile": Param(
                type=["null", "string"],
                title="[MATSim] inputChangeEventsFile path"
            ),
            "MATSim_datapath_transitScheduleFile": Param(
                type=["null", "string"],
                title="[MATSim] transitScheduleFile path"
            ),
            "MATSim_datapath_transitLinesAttributesFile": Param(
                type=["null", "string"],
                title="[MATSim] transitLinesAttributesFile"
            ),
            "MATSim_datapath_transitStopsAttributesFile": Param(
                type=["null", "string"],
                title="[MATSim] transitStopsAttributesFile"
            ),
            "MATSim_datapath_laneDefinitionsFile": Param(
                type=["null", "string"],
                title="[MATSim] laneDefinitionsFile"
            ),
            "MATSim_datapath_inputPersonAttributesFile": Param(
                type=["null", "string"],
                title="[MATSim] inputPersonAttributesFile"
            ),
            "MATSim_datapath_inputAlightCountsFile": Param(
                type=["null", "string"],
                title="[MATSim] inputAlightCountsFile"
            ),
            "MATSim_datapath_inputBoardCountsFile": Param(
                type=["null", "string"],
                title="[MATSim] inputBoardCountsFile"
            ),
            "MATSim_datapath_inputOccupancyCountsFile": Param(
                type=["null", "string"],
                title="[MATSim] inputOccupancyCountsFile"
            )
        }
        return self._modify_items(data, exclude, changes)

    def floodEvent(self, exclude=None, changes=None):
        data = {
            "floodEvent_datapath_config": Param(
                type="string",
                title="[floodEvent] config path",
                description=F"Path to the config.json file for flood-event module (include bucket name). E.g. {os.getenv('FLOODEVENT_DATAPATH_CONFIG_DEFAULT')}",
            ),
            "floodEvent_datadir_flood-rasters": Param(
                type="string",
                title="[floodEvent] rasters directory",
                description=f"Path to the DIRECTORY containing flood rasters. These rasters must be in '.tif' format and numbered sequentially e.g. 0.tif, 1.tif, 2.tif etc. Include bucket name. E.g. {os.getenv('FLOODEVENT_DATADIR_FLOODRASTERS_DEFAULT')}"
            ),
            "floodEvent_datapath_network": Param(
                type="string",
                title="[floodEvent] network path",
                description=f"Path to the network.gpkg file. Include bucket name. E.g. {os.getenv('FLOODEVENT_DATAPATH_NETWORK_DEFAULT')}"
            )
        }
        return self._modify_items(data, exclude, changes)

    def floodEvent_matsim(self):
        new_params = {
            "MATSim_datapath_inputChangeEventsFile": Param(
                default=os.getenv('MATSIM_DATAPATH_NETWORKCHANGEEVENTS_FLOODEVENT'),
                type="string",
                title="[MATSim-CHAIN] LEAVE AS DEFAULT",
                description="Network change events to be inherited by floodEvent module."
            )
        }
        return self.floodEvent() | self.matsim(exclude=["datapath_networkChangeEvents"]) | new_params