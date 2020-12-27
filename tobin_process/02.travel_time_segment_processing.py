import pandas as pd
import os
import glob
from tobin_process.utils import get_project_root
import tobin_process.travel_time_seg_helper as tt_helper

if __name__ == "__main__":
    # 1. Set the paths for input files and output files.
    # ************************************************************************************
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")
    path_to_mappers_data = os.path.join(path_to_prj, "data", "mappers")
    path_to_mapper_tt_seg = os.path.join(path_to_mappers_data, "tt_seg_mapping.xlsx")
    paths_tt_vissim_raw = glob.glob(
        os.path.join(path_to_raw_data, "AM_Raw Travel Time", "*.rsr")
    )
    paths_data_col_vissim_raw = glob.glob(
        os.path.join(path_to_raw_data, "AM_Raw Travel Time", "*.mer")
    )
    path_to_output_tt = os.path.join(path_to_interim_data, "process_tt.xlsx")
    path_to_output_fig = os.path.join(path_to_interim_data, "figures")
    if not os.path.exists(path_to_output_fig):
        os.mkdir(path_to_output_fig)
    path_to_output_tt_fig = os.path.join(path_to_output_fig, "am_figures_tt_seg")
    if not os.path.exists(path_to_output_tt_fig):
        os.mkdir(path_to_output_tt_fig)

    # 2. Set time interval, time interval labels, report vehicle classes mapping to
    # vehicle types, occupancy by vissim vehicle type, results column to retain,
    # travel time segments to keep.
    # in results.
    # ************************************************************************************
    # Vissim time intervals
    order_timeint = [
        "2700-3600",
        "3600-4500",
        "4500-5400",
        "5400-6300",
        "6300-7200",
        "7200-8100",
        "8100-9000",
        "9000-9900",
        "9900-10800",
        "10800-11700",
        "11700-12600",
        "12600-13500",
        "13500-14400",
    ]
    # Vissim time interval labels.
    order_timeint_labels_am = [
        "6:00-6:15",
        "6:15-6:30",
        "6:30-6:45",
        "6:45-7:00",
        "7:00-7:15",
        "7:15-7:30",
        "7:30-7:45",
        "7:45-8:00",
        "8:00-8:15",
        "8:15-8:30",
        "8:30-8:45",
        "8:45-9:00",
        "9:00-9:15",
    ]
    # Vissim time interval labels for pm.
    order_timeint_labels_pm = [
        "4:00-4:15",
        "4:15-4:30",
        "4:30-4:45",
        "4:45-5:00",
        "5:00-5:15",
        "5:15-5:30",
        "5:30-5:45",
        "5:45-6:00",
        "6:00-6:15",
        "6:15-6:30",
        "6:30-6:45",
        "6:45-7:00",
        "7:00-7:15",
    ]
    # Report vehicle classes and corresponding vissim vehicle types.
    veh_types_res_cls = {
        "car_hgv_bus": [100, 200, 300, 301, 302, 303, 304, 305],
        "car_hgv": [100, 200],
        "bus": [300, 301, 302, 303, 304, 305],
    }

    # Which data collection points to use for vehicle occupancy?
    use_data_col_no = [3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007, 3008]
    # Columns to keep.
    keep_cols = ["time", "no", "veh", "veh_type", "trav", "delay", "dist"]
    # Result columns.
    results_cols = [
        "avg_trav",
        "avg_speed",
        "q95_trav",
        "avg_veh_delay",
        "avg_pers_delay",
        "tot_veh",
        "tot_pers",
        "avg_dist_ft",
    ]
    # Result travel time segment number to be retained in the output.
    # [1,2,3,4,5,6,7,8,9,10,11,12]
    keep_tt_segs = [1, 23, 4, 20, 24, 21, 11, 12, 13, 25]

    # Which Travel time segments to include in travel time results
    plot_tt_segs = [1, 23, 4, 20, 21, 11, 12, 13]
    tt_eval_am = tt_helper.TtEval(
        path_to_mapper_tt_seg_=path_to_mapper_tt_seg,
        paths_tt_vissim_raw_=paths_tt_vissim_raw,
        path_output_tt_=path_to_output_tt,
        path_to_output_tt_fig_=path_to_output_tt_fig,
    )

    # Read the raw rsr files, filter rows and columns, combine data from different runs
    # and get summary statistics for each simulation run.
    # Delete the following if you do not want to incoporate occupancy data from data
    # collection points:
    #     paths_data_col_vissim_raw_ = paths_data_col_vissim_raw,
    #     use_data_col_no_ = use_data_col_no,
    #     use_data_col_res = True,
    #     car_hgv_veh_occupancy = 1.3,
    # TODO: Make the read_rsr_tt and get_person_delay_from_data_col_raw_data_bus_occupancy more
    #  flexible.
    #  Currently it makes assumption about what vehicle types are buses; all vehicle type
    #  below 300 are considered cars and HGVs and all vehicle type above or equal to 300
    #  are considered buses.
    #  Let user define what vehicle type is a bus. I (Apoorb) have hard coded this for
    #  Tobin Bridge.
    tt_eval_am.read_rsr_tt(
        order_timeint_=order_timeint,
        order_timeint_labels_=order_timeint_labels_am,
        veh_types_res_cls_=veh_types_res_cls,
        keep_cols_=keep_cols,
        keep_tt_segs_=keep_tt_segs,
        paths_data_col_vissim_raw_=paths_data_col_vissim_raw,
        use_data_col_no_=use_data_col_no,
        use_data_col_res=True,
        car_hgv_veh_occupancy=1.3,
    )
    # Add travel time segment name and direction to the data with summary statistics for
    # each simulation run.
    tt_eval_am.merge_mapper()
    # Aggregate travel time results to get an average of all simulation runs.
    tt_eval_am.agg_tt(results_cols_=results_cols)
    tt_eval_am.save_tt_processed()
    tt_eval_am.plot_heatmaps(segs_to_plot=plot_tt_segs, var="avg_speed")
