import pandas as pd
import os
import glob
from tobin_process.utils import get_project_root
import tobin_process.travel_time_seg_helper as tt_helper


if __name__ == "__main__":
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")
    path_to_mappers_data = os.path.join(path_to_prj, "data", "mappers")
    path_to_mapper_tt_seg = os.path.join(path_to_mappers_data, "tt_seg_mapping.xlsx")
    path_tt_vissim = os.path.join(
        path_to_raw_data, "Tobin Bridge Base Model_Vehicle Travel Time Results.att"
    )
    paths_tt_vissim_raw = glob.glob(
        os.path.join(path_to_raw_data, "AM_Raw Travel Time", "*.rsr")
    )
    path_to_output_tt = os.path.join(path_to_interim_data, "process_tt.xlsx")
    path_to_output_fig = os.path.join(path_to_interim_data, "figures")
    if not os.path.exists(path_to_output_fig):
        os.mkdir(path_to_output_fig)
    path_to_output_tt_fig = os.path.join(path_to_output_fig, "am_figures_tt_seg")
    if not os.path.exists(path_to_output_tt_fig):
        os.mkdir(path_to_output_tt_fig)
    order_timeint = [
        "900-1800",
        "1800-2700",
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
    ]

    order_timeint_labels = [
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
        "7:00-7:15"
    ]
    order_timeint_intindex = pd.IntervalIndex.from_tuples(
        [
            (int(timeint.split("-")[0]), int(timeint.split("-")[1]))
            for timeint in order_timeint
        ],
        closed="left",
    )

    veh_types_res_cls = {
        "car": [100],
        "hgv": [200],
        "car_hgv": [100, 200],
        "bus": [300],
        "car_hgv_bus": [100, 200, 300],
    }
    veh_types_occupancy = {100: 1, 200: 1, 300: 60}
    keep_cols = ["time", "no", "veh", "veh_type", "trav", "delay", "dist"]
    results_cols = [
        "avg_trav",
        "avg_speed",
        "q95_trav",
        "avg_veh_delay",
        "avg_person_delay",
        "tot_veh",
        "tot_people",
    ]
    keep_tt_segs = range(1, 12 + 1)
    tt_eval_am = tt_helper.TtEval(
        path_to_prj_=path_to_prj,
        path_to_raw_data_=path_to_raw_data,
        path_to_interim_data_=path_to_interim_data,
        path_to_mapper_tt_seg_=path_to_mapper_tt_seg,
        paths_tt_vissim_raw_=paths_tt_vissim_raw,
        path_output_tt_=path_to_output_tt,
        path_to_output_tt_fig_=path_to_output_tt_fig,
    )

    tt_eval_am.read_rsr_tt(
        order_timeint_intindex_=order_timeint_intindex,
        order_timeint_labels_=order_timeint_labels,
        veh_types_occupancy_=veh_types_occupancy,
        veh_types_res_cls_=veh_types_res_cls,
        keep_cols_=keep_cols,
        keep_tt_segs_=keep_tt_segs,
    )

    tt_eval_am.merge_mapper_grp()
    tt_eval_am.agg_tt(results_cols_=results_cols)
    tt_eval_am.save_tt_processed()
    tt_eval_am.plot_heatmaps()
