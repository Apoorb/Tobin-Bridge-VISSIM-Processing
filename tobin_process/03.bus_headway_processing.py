import pandas as pd
import os
import glob
from tobin_process.utils import get_project_root
import tobin_process.bus_headway_helper as bus_helper

if __name__ == "__main__":
    # 1. Set the paths for input files and output files.
    # ************************************************************************************
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")
    path_to_mappers_data = os.path.join(path_to_prj, "data", "mappers")
    path_to_mapper_bus_headway = os.path.join(
        path_to_mappers_data, "bus_headway_mapping.xlsx"
    )
    paths_tt_vissim_raw = glob.glob(
        os.path.join(path_to_raw_data, "AM_Raw Travel Time", "*.rsr")
    )
    path_to_output_headway = os.path.join(path_to_interim_data, "process_headway.xlsx")
    # 2. Set time interval, time interval labels, report vehicle classes mapping to
    # vehicle types, occupancy by vissim vehicle type, results column to retain,
    # travel time segments to keep.
    # in results.
    # ************************************************************************************
    # Vissim time intervals
    order_timeint = ["2700-6300", "6300-9900", "9900-13500", "13500-14400"]
    # Vissim time interval labels for am.
    order_timeint_labels_am = ["6:00-7:00", "7:00-8:00", "8:00-9:00", "9:00-9:15"]
    # Vissim time interval labels for pm.
    order_timeint_labels_pm = ["4:00-5:00", "5:00-6:00", "6:00-7:00", "7:00-7:15"]
    # Report vehicle classes and corresponding vissim vehicle types.
    veh_types_res_cls = {
        "MBTA-111": [301],
        "MBTA-426-426W-428": [302, 303, 304],
        "Private Bus": [305]
    }
    # Occupany by vissim vehicle types.
    veh_types_occupancy = {100: 1, 200: 1, 300: 60}
    # Columns to keep.
    keep_cols = ["time", "no", "veh", "veh_type", "trav", "delay", "dist"]
    # Result columns.
    results_cols = [
        "avg_trav",
        "avg_speed",
        "q95_trav",
        "avg_veh_delay",
        "avg_person_delay",
        "tot_veh",
        "tot_people",
    ]
    # Result travel time segment number to be retained in the output.
    # 1,
    keep_tt_segs = (101, 102, 103, 104, 105, 106)

    bus_headway_am = bus_helper.BusHeadway(
        paths_tt_vissim_raw_=paths_tt_vissim_raw,
        path_to_mapper_bus_headway_=path_to_mapper_bus_headway,
        path_to_output_headway_=path_to_output_headway,
    )
    # Read the raw rsr files, filter rows and columns, combine data from different runs
    # and get summary statistics for each simulation run.
    bus_headway_am.read_rsr_tt(
        order_timeint_=order_timeint,
        order_timeint_labels_=order_timeint_labels_am,
        veh_types_occupancy_=veh_types_occupancy,
        veh_types_res_cls_=veh_types_res_cls,
        keep_cols_=keep_cols,
        keep_tt_segs_=keep_tt_segs,
    )
    # Add travel time segment name and direction to the raw data for each simulation
    # run.
    bus_headway_am.merge_mapper()
    bus_headway_am.get_headway_stats()
    bus_headway_am.save_headway()
