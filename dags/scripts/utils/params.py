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
            "floodEvent_datadir_flood-rasters": Param(
                type="string",
                title="[floodEvent] rasters directory",
                description=f"Path to the DIRECTORY containing flood rasters. These rasters must be in '.tif' format and numbered sequentially e.g. 0.tif, 1.tif, 2.tif etc. Include bucket name. E.g. {os.getenv('FLOODEVENT_DATADIR_FLOODRASTERS_DEFAULT')}"
            ),
            "floodEvent_datapath_network": Param(
                type="string",
                title="[floodEvent] network path",
                description=f"Path to the network.gpkg file. Include bucket name. E.g. {os.getenv('FLOODEVENT_DATAPATH_NETWORK_DEFAULT')}"
            ),
            "floodEvent_datavalue_crs": Param(
                "27700",
                type="string",
                title="[floodEvent] CRS"
            ),
            "floodEvent_datavalue_network_from": Param(
                "PT2",
                type="string",
                title="[floodEvent] network_from"
            ),
            "floodEvent_datavalue_network_buffer_factor": Param(
                1.825,
                type="number",
                title="[floodEvent] network_buffer_factor"
            ),
            "floodEvent_datavalue_excluded_modes": Param(
                type=["null", "array"],
                title="[floodEvent] excluded_modes",
                description='Mode links to exclude. To create a list input each item on its own line with no extra symbols or leading/trailing whitespaces'
            ),
            "floodEvent_datavalue_depth_statistic": Param(
                "max",
                type="string",
                title="[floodEvent] depth_statistic",
                description="For options see: https://isciences.github.io/exactextract/operations.html"
            ),
            "floodEvent_datavalue_event_start_time": Param(
                type="string",
                title="[floodEvent] event_start_time",
                description="Event start time as format %H:%M:%S (e.g. 12:00:00 for 12pm)"
            ),
            "floodEvent_datavalue_time_interval": Param(
                type="string",
                title="[floodEvent] time_interval",
                description="Interval time as format %H:%M:%S (e.g. 00:10:00 for a 10 min interval)"
            ),
        }
        return self._modify_items(data, exclude, changes)

    def udmSetup(self, exclude=None, changes=None):
        data = {
            "udm-setup_selection_image": Param(
                "udm-setup:0fef60f27f1ecd4ad0dfefba5aeb374d26a9abf0",
                type="string",
                title="[udm-setup] Docker Image",
                description="Select an udm-setup Docker image, as <name:tag>. See available images or upload your own (in .tar format) at docker-images bucket.",
            ),
            # TODO: Is this the same as "OpenUDM_datapath_developedland_coverage" when coupling?
            "udm-setup_datapath_developed_coverage": Param(
                type="string",
                title="[udm-setup] developed_coverage path",
            ),
            "udm-setup_datapath_metadata": Param(
                type="string",
                title="[udm-setup] metadata path",
            ),
            "udm-setup_datapath_population": Param(
                type="string",
                title="[udm-setup] population path",
            ),
            "udm-setup_datapath_roads_proximity": Param(
                type="string",
                title="[udm-setup] roads_proximity path",
            ),
            "udm-setup_datapath_zone_identity": Param(
                type="string",
                title="[udm-setup] zone_identity path",
            ),
            "udm-setup_datavalue_attractors": Param(
                type="string",
                title="[udm-setup] attractors parameters",
                description="A semi-colan seperated list of attractor layers (or string similar in some part) including a polarity value and a weight value. e.g: 'road:0.3:1;'",
            ),
            "udm-setup_datavalue_constraints": Param(
                type="string",
                title="[udm-setup] constraints parameters",
                description="A semi-colan seperated list of constraint layers (or string similar in some part) including a threshold value for the layer at which point the cell is classed occupied/full and not suitable for development. All layers cell values should relate to a 'coverage' value, sucha s genereted by the udm-rasterise-coverage tool/model. e.g.: 'developed:0.3;'",
            ),
            "udm-setup_datavalue_current_development": Param(
                type="string",
                title="[udm-setup] current_development parameters",
                description="A colan seperated list of a file name (or string similar in some part) and a threshold value for % coverage denoting the pint it's considered already occupied/developed. e.g.: 'developed:0.3;'",
            ),
            "udm-setup_datavalue_density_from_raster": Param(
                type="integer",
                title="[udm-setup] density_from_raster parameters",
                description="Parameter (integer) to identify is new development density is taken from the input data (0) or from an input density raster.",
            ),
            "udm-setup_datavalue_people_per_dwelling": Param(
                type="number",
                title="[udm-setup] people_per_dwelling parameters",
                description="Float value for converting between people per cell and people per dwelling.",
            ),
            "udm-setup_datavalue_coverage_threshold": Param(
                type="number",
                title="[udm-setup] coverage_threshold parameters",
                description="Float value for the threshold value at which a cell is considered occupied/full and can't be developed when all constraints have been merged.",
            ),
            "udm-setup_datavalue_minimum_development_area": Param(
                type="integer",
                title="[udm-setup] minimum_development_area parameters",
                description="Integer value for the minimum number of cells which must be clustered together to consider for development.",
            ),
            "udm-setup_datavalue_maximum_plot_size": Param(
                type="integer",
                title="[udm-setup] maximum_plot_size parameters",
                description="Integer value for the maximum number of cells developed around most suitable cell in zone in each iteration of development spreading algorithm.",
            ),
            "udm-setup_datavalue_extra_parameters": Param(
                type=["null", "string"],
                title="[udm-setup] extra_parameters parameters",
                description="Allow alternative parameters to be noted, such as those based on the inclusion of weightings of layers.",
            ),
        }
        return self._modify_items(data, exclude, changes)

    def OpenUDM(self, exclude=None, changes=None):
        data = {
            "OpenUDM_selection_image": Param(
                "udm-dafni:4b3634324e3985bbfa645c7129cc6f0159fe6e82",
                type="string",
                title="[OpenUDM] Docker Image",
                description="Select an OpenUDM Docker image, as <name:tag>. See available images or upload your own (in .tar format) at docker-images bucket.",
            ),
            "OpenUDM_datapath_attractors": Param(
                type="string",
                title="[OpenUDM] attractors path",
                description="Path to the attractors.csv file for OpenUDM model (include bucket name)"
            ),
            "OpenUDM_datapath_constraints": Param(
                type="string",
                title="[OpenUDM] constraints path",
                description="Path to the constraints.csv file for OpenUDM model (include bucket name)"
            ),
            "OpenUDM_datapath_developedland_coverage_100m": Param(
                type="string",
                title="[OpenUDM] developedland_coverage_100m path",
                description="Path to the developedland_coverage.asc file for OpenUDM model (include bucket name)"
            ),
            "OpenUDM_datapath_development_proximity_100m": Param(
                type="string",
                title="[OpenUDM] development_proximity_100m path",
                description="Path to the development_proximity.asc file for OpenUDM model (include bucket name)"
            ),
            "OpenUDM_datapath_parameters": Param(
                type="string",
                title="[OpenUDM] parameters path",
                description="Path to the parameters.csv file for OpenUDM model (include bucket name)"
            ),
            "OpenUDM_datapath_population": Param(
                type="string",
                title="[OpenUDM] population path",
                description="Path to the population.csv file for OpenUDM model (include bucket name)"
            ),
            "OpenUDM_datapath_zone_identity": Param(
                type="string",
                title="[OpenUDM] zone_identity path",
                description="Path to the zone_identity.csv file for OpenUDM model (include bucket name)"
            ),
            "OpenUDM_datapath_metadata": Param(
                type="string",
                title="[OpenUDM] metadata path",
                description="Path to the metadata.csv produced by udm-setup (include bucket name)"
            ),
            "OpenUDM_datavalue_run_ufg": Param(
                type="boolean",
                title="[OpenUDM] Enable Urban Fabric Generator?"
            ),
        }
        return self._modify_items(data, exclude, changes)

    def citycat(self, exclude=None, changes=None):
        data = {
            "CityCAT_selection_image": Param(
                "citycat:6.4.16.8604",
                type="string",
                title="[CityCAT] Docker Image",
                description="Select a CityCAT Docker image, as <name:tag>. See available images or upload your own (in .tar format) at docker-images bucket.",
            ),
            "CityCAT_datapath_CityCat_Config_1": Param(
                type="string",
                title="[CityCAT] CityCat_Config path",
                description="Your CityCat_Config and Rainfall_Data filenames will be renamed to CityCat_Config_1 and Rainfall_Data_1, with output name R1C1_SurfaceMaps"
            ),
            "CityCAT_datapath_Rainfall_Data_1": Param(
                type="string",
                title="[CityCAT] Rainfall_Data path",
                description="Your CityCat_Config and Rainfall_Data filenames will be renamed to CityCat_Config_1 and Rainfall_Data_1, with output name R1C1_SurfaceMaps"
            ),
            "CityCAT_datapath_Buildings": Param(
                type="string",
                title="[CityCAT] Buildings path",
            ),
            "CityCAT_datapath_Domain_DEM": Param(
                type="string",
                title="[CityCAT] Domain_DEM path",
            ),
            "CityCAT_datapath_GreenAreas": Param(
                type="string",
                title="[CityCAT] GreenAreas path",
            ),
        }
        return self._modify_items(data, exclude, changes)

    def floodEvent_matsim(self):
        new_params = {
            "MATSim_datapath_inputChangeEventsFile": Param(
                default=os.getenv('MATSIM_DATAPATH_NETWORKCHANGEEVENTS_FLOODEVENT'),
                type="string",
                title="[MATSim-CHAIN] LEAVE AS DEFAULT",
                description="Network change events to be inherited from floodEvent module."
            )
        }
        return self.floodEvent() | self.matsim(exclude=["datapath_networkChangeEvents"]) | new_params

    def udmSetup_OpenUDM(self):
        new_params = {
            "OpenUDM_datapath_attractors": Param(
                default=os.getenv('OPENUDM_DATAPATH_ATTRACTORS_UDMSETUP'),
                type="string",
                title="[OpenUDM-CHAIN] LEAVE AS DEFAULT",
                description="attractors to be inherited from udm-setup module."
            ),
            "OpenUDM_datapath_constraints": Param(
                default=os.getenv('OPENUDM_DATAPATH_CONSTRAINTS_UDMSETUP'),
                type="string",
                title="[OpenUDM-CHAIN] LEAVE AS DEFAULT",
                description="constraints to be inherited from udm-setup module."
            ),
            "OpenUDM_datapath_parameters": Param(
                default=os.getenv('OPENUDM_DATAPATH_PARAMETERS_UDMSETUP'),
                type="string",
                title="[OpenUDM-CHAIN] LEAVE AS DEFAULT",
                description="parameters to be inherited from udm-setup module."
            ),
            "OpenUDM_datapath_population": Param(
                default=os.getenv('OPENUDM_DATAPATH_POPULATION_UDMSETUP'),
                type="string",
                title="[OpenUDM-CHAIN] LEAVE AS DEFAULT",
                description="population to be inherited from udm-setup module."
            ),
            "OpenUDM_datapath_zone_identity": Param(
                default=os.getenv('OPENUDM_DATAPATH_ZONEIDENTITY_UDMSETUP'),
                type="string",
                title="[OpenUDM-CHAIN] LEAVE AS DEFAULT",
                description="zone_identity to be inherited from udm-setup module."
            ),
            "OpenUDM_datapath_metadata": Param(
                default=os.getenv('OPENUDM_DATAPATH_METADATA_UDMSETUP'),
                type="string",
                title="[OpenUDM-CHAIN] LEAVE AS DEFAULT",
                description="metadata to be inherited from udm-setup module."
            )
        }
        return self.udmSetup() | self.OpenUDM(exclude=[
            "OpenUDM_datapath_attractors",
            "OpenUDM_datapath_constraints",
            "OpenUDM_datapath_parameters",
            "OpenUDM_datapath_population",
            "OpenUDM_datapath_zone_identity",
            "OpenUDM_datapath_metadata"
        ]) | new_params