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
        # "input_bucket": Param(
        #     "models",
        #     type="string",
        #     title="MinIO input bucket",
        #     enum=["models"]
        # ),
        # "output_bucket": Param(
        #     "models",
        #     type="string",
        #     title="MinIO output bucket",
        #     enum=["models"]
        # ),
        data = {
            "scenario_name": Param(
                type=["null", "string"],
                title="Scenario name",
                # description="Create or reference the name of this scenario."
                description="(optional) Name this scenario."
            ),
            # "scenario_prefix": Param(
            #     False,
            #     type="boolean",
            #     title="Scenario prefix?",
            #     description="If this is selected, airflow will look for files prefixed with your scenario name. When naming files, '_' are only allowed at the end of the prefix as a separator, e.g. 'flood-event-1_config.xml'. If files are in default folders, leave all following paths as their default values."
            # ),
        }
        return self._modify_items(data, exclude, changes)

    def matsim(self, exclude=None, changes=None):
        data = {
            "MATSim_selection_image": Param(
                "matsim:15.0-2022w40",
                type="string",
                title="[MATSim] Docker Image",
                description="Select a MATSim Docker image.",
                # TODO: Dynamically load images
                enum=["matsim:15.0-2022w40", "matsim:15.0-PR2396"],
            ),
            "MATSim_datapath_config": Param(
                type="string",
                title="[MATSim] config path",
                description=f"PATH to the config.xml file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_CONFIG_DEFAULT')}"
            ),
            "MATSim_datapath_network": Param(
                type="string",
                title="[MATSim] network path",
                description=f"PATH to the network .[xml, xml.gz] file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_NETWORK_DEFAULT')}"
            ),
            "MATSim_datapath_plans": Param(
                type="string",
                title="[MATSim] plans path",
                description=f"PATH to the plans .[xml, xml.gz] file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_PLANS_DEFAULT')}"
            ),
            "MATSim_datapath_transitSchedule": Param(
                type=["null", "string"],
                title="[MATSim] transit schedule path",
                description=f"PATH to the transit schedule .[xml, xml.gz] file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_TRANSITSCHEDULE_DEFAULT')}"
            ),
            "MATSim_datapath_transitVehicles": Param(
                type=["null", "string"],
                title="[MATSim] transit vehicles path",
                description=f"PATH to the transit vehicles .xml file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_TRANSITVEHICLES_DEFAULT')}"
            ),
            "MATSim_datapath_motor_veh_counts": Param(
                type=["null", "string"],
                title="[MATSim] vehicle motor counts path",
                description=f"PATH to the motor counts .xml file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_MOTORVEHCOUNTS_DEFAULT')}"
            ),
            "MATSim_datapath_cyclePedal_veh_counts": Param(
                type=["null", "string"],
                title="[MATSim] vehicle PedalCycle counts path",
                description=f"PATH to the PedalCycle counts .xml file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_CYCLEPEDALVEHCOUNTS_DEFAULT')}"
            ),
            "MATSim_datapath_networkChangeEvents": Param(
                type=["null", "string"],
                title="[MATSim] network change events path",
                description=f"PATH to the network change events .xml file for MATSim (include bucket name). E.g. {os.getenv('MATSIM_DATAPATH_NETWORKCHANGEEVENTS_DEFAULT')}"
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
            "floodEvent_datadir_network": Param(
                type="string",
                title="[floodEvent] network directory",
                description=f"Path to the DIRECTORY containing the network shapefile. Include in this directory supporting files '.cpg', '.dbf', '.prj', '.qmd', and '.shx'. Include bucket name. E.g. {os.getenv('FLOODEVENT_DATADIR_NETWORK_DEFAULT')}"
            )
        }
        return self._modify_items(data, exclude, changes)

    def floodEvent_matsim(self):
        new_params = {
            "MATSim_datapath_networkChangeEvents": Param(
                default=os.getenv('MATSIM_DATAPATH_NETWORKCHANGEEVENTS_FLOODEVENT'),
                type="string",
                title="[MATSim-CHAIN] LEAVE AS DEFAULT",
                description="Network change events to be inherited by floodEvent module."
            )
        }
        return self.floodEvent() | self.matsim(exclude=["datapath_networkChangeEvents"]) | new_params