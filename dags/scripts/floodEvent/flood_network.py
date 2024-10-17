# This script assigns flood depths and subsequent vehicle velocities to each
# link within a network.
# Inputs - cityCAT floodmap output directory, transport network


import os
import fiona
import pyogrio  # utilised for faster exporting
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from rasterstats import zonal_stats
import json


def load_config(filepath):
    with open(filepath, 'r') as file:
        return json.load(file)


def load_network(filepath, CRS):
    with fiona.open(filepath) as src:
        geometries = [shape(feature['geometry']) for feature in src]
        properties = [feature['properties'] for feature in src]
    df = gpd.GeoDataFrame(properties, geometry=geometries)
    return df.set_crs(CRS)


def exlude_network_modes(df, excluded_modes):
    return df.loc[~df["MODES"].isin(excluded_modes)]


def buffer_network(df, factor):
    df["geometry"] = df.buffer(df["LANES"] * factor)
    return df


def remove_false_positive_categories(df):
    # TODO: Remove bridges (and maybe connecting links)
    pass


def prepare_network(network_filepath, excluded_modes, network_buffer_factor, CRS):
    print("Preparing network")
    gdf = load_network(network_filepath, CRS)
    gdf = exlude_network_modes(gdf, excluded_modes)
    return buffer_network(gdf, network_buffer_factor)


def zonal_statistics(gdf, filepath):
    print("calculating zonal statistics")
    return gdf.join(
        pd.DataFrame(
            zonal_stats(
                vectors=gdf['geometry'],
                raster=filepath,
                stats=["count", "min", "max", "mean", "sum", "std", "median",
                       "majority", "minority", "unique", "range"],
                all_touched=True
            )
        ),
        how='left'
    )


def calculate_velocity(depth, freespeed):
    v_kmh = (0.0009 * depth**2) - (0.5529 * depth) + 86.9448
    v_ms = v_kmh / 3.6
    if v_ms > freespeed:
        return freespeed
    return v_ms


# Method: https://doi.org/10.1016/j.trd.2017.06.020
def vehicle_velocity(gdf, link_depth):
    print("calculating vehicle velocities")
    # Convert depth value from m to mm: * 1000
    gdf["velocity"] = gdf.apply(
        lambda row: calculate_velocity(row[link_depth]*1000, row["FRSPEED"]),
        axis=1
    )
    return gdf


def export_gpkg(gdf, filepath):
    print("Exporting to file: ", filepath + ".gpkg")
    gdf.to_file(filepath + ".gpkg", driver="GPKG", engine="pyogrio")


def export_csv(gdf, filepath):
    print("Exporting to file: ", filepath + ".csv")
    # TODO: Instead of dropping geometry, turn to WKT
    # gdf['geometry'] = gdf['geometry'].apply(
    #     lambda geom: geom.wkt if geom else None)
    df = gdf.drop(columns='geometry')
    df = df.fillna("null")
    df.to_csv(filepath + ".csv", index=False)


def main(config_filepath, network_dir, floodmap_dir, output_dir):
    config = load_config(config_filepath)["flood_network"]

    # Handles any network shapefile name
    for filename in os.listdir(network_dir):
        if filename.endswith(".shp"):
            network_filepath = network_dir + filename

    gdf_network = prepare_network(
        network_filepath,
        config["excluded_modes"],
        config["network_buffer_factor"],
        config["CRS"]
    )

    for file in os.listdir(floodmap_dir):
        if not file.endswith(config["extension"]):
            continue

        print("Processing: ", file)

        # TODO: Standardise input and output naming
        floodmap_filepath = floodmap_dir + file
        output_filepath = output_dir + file.replace(".tif", "_flooded_network")

        gdf = zonal_statistics(gdf_network, floodmap_filepath)
        gdf = vehicle_velocity(gdf, config["link_depth"])

        export_gpkg(gdf, output_filepath)
        export_csv(gdf, output_filepath)

    print("Done")


if __name__ == "__main__":
    main(
        config_filepath="",
        floodmap_dir="",
        network_dir="",
        output_dir=""
    )