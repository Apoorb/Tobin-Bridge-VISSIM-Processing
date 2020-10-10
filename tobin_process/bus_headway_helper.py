import pandas as pd
import numpy as np
import os
import glob
import inflection
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root
import tobin_process.travel_time_seg_helper as tt_helper


class BusHeadway(tt_helper.TtEval):
    def __init__(
        self,
        path_to_prj_,
        path_to_raw_data_,
        path_to_interim_data_,
        paths_tt_vissim_raw_,
        path_to_mapper_bus_headway_,
        path_to_output_headway_
    ):
        self.path_to_output_headway = path_to_output_headway_
        self.tt_vissim_headway_grp = pd.DataFrame()
        super().__init__(
            path_to_prj_=path_to_prj_,
            path_to_raw_data_=path_to_raw_data_,
            path_to_interim_data_=path_to_interim_data_,
            path_to_mapper_tt_seg_=path_to_mapper_bus_headway_,
            paths_tt_vissim_raw_=paths_tt_vissim_raw_,
            path_output_tt_="",
            path_to_output_tt_fig_="",
        )

    def get_headway_stats(self):
        tt_vissim_headway = (
            self.tt_vissim_raw
            .sort_values(["direction", "tt_seg_name", "veh_cls_res", "timeint", "time"])
            .assign(
                headway=lambda df:
                df.groupby(["direction", "tt_seg_name", "veh_cls_res"])["time"].diff()
            )
            .filter(items=[
                "direction", "tt_seg_name", "veh_cls_res", "timeint", "headway"])
        )

        self.tt_vissim_headway_grp = (
            tt_vissim_headway
            .groupby(["direction", "tt_seg_name", "veh_cls_res", "timeint"])
            .agg(
                avg_headway=("headway", "mean"),
                min_headway=("headway", "min"),
                q50_headway=("headway", lambda x: np.quantile(x, 0.5)),
                q95_headway=("headway", lambda x: np.quantile(x, 0.95)),
                max_headway=("headway", "max"),
                std_dev_headway=("headway", "std"),
                coeff_var_headway=("headway", lambda x: x.std() / x.mean()),
            )
            .reset_index()
        )

    def save_headway(self):
        self.tt_vissim_headway_grp.to_excel(self.path_to_output_headway)


if __name__ == "__main__":
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

    order_timeint = [
        "900-4500",
        "4500-8100",
        "8100-11700",
        "11700-12600",
    ]

    order_timeint_labels = [
        "6:00-7:00",
        "7:00-8:00",
        "8:00-9:00",
        "9:00-9:15"
    ]

    order_timeint_labels_pm = [
        "4:00-5:00",
        "5:00-6:00",
        "6:00-7:00",
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
        "bus": [300],
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
    keep_tt_segs = range(101, 106 + 1)
    bus_headway_am = BusHeadway(
        path_to_prj_=path_to_prj,
        path_to_raw_data_=path_to_raw_data,
        path_to_interim_data_=path_to_interim_data,
        paths_tt_vissim_raw_=paths_tt_vissim_raw,
        path_to_mapper_bus_headway_=path_to_mapper_bus_headway,
        path_to_output_headway_=path_to_output_headway
    )

    bus_headway_am.read_rsr_tt(
        order_timeint_intindex_=order_timeint_intindex,
        order_timeint_labels_=order_timeint_labels,
        veh_types_occupancy_=veh_types_occupancy,
        veh_types_res_cls_=veh_types_res_cls,
        keep_cols_=keep_cols,
        keep_tt_segs_=keep_tt_segs,
    )

    bus_headway_am.merge_mapper()
    bus_headway_am.get_headway_stats()
    bus_headway_am.save_headway()
