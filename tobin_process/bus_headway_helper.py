import pandas as pd
import numpy as np
import os
import glob
from tobin_process.utils import get_project_root
import tobin_process.travel_time_seg_helper as tt_helper


class BusHeadway(tt_helper.TtEval):
    def __init__(
        self,
        paths_tt_vissim_raw_,
        path_to_mapper_bus_headway_,
        path_to_output_headway_,
    ):
        """
        Parameters
        ----------
        paths_tt_vissim_raw_: str
            Full path to vissim files from different runs.
        path_to_mapper_bus_headway_: str
            Path to the bus headway mapper file.
        path_to_output_headway_: str
            Path to output the processed bus headway results.
        """
        self.path_to_output_headway = path_to_output_headway_
        self.tt_vissim_headway_grp = pd.DataFrame()
        super().__init__(
            path_to_mapper_tt_seg_=path_to_mapper_bus_headway_,
            paths_tt_vissim_raw_=paths_tt_vissim_raw_,
            path_output_tt_="",
            path_to_output_tt_fig_="",
        )

    def get_headway_stats(self):
        """
        Get headway statistics by using the data from all vissim runs.
        """
        # Get headway by individual vissim run.
        # TODO: Figure out the aggregation variables. Should we use veh_type or
        #  veh_cls_res?
        tt_vissim_headway = (
            self.tt_vissim_raw.sort_values(
                ["run_no", "direction", "tt_seg_name", "veh_cls_res", "time",]
            )
            .assign(
                headway=lambda df: df.groupby(
                    ["run_no", "direction", "tt_seg_name", "veh_cls_res"]
                )["time"].diff()
            )
            .filter(
                items=[
                    "run_no",
                    "direction",
                    "tt_seg_name",
                    "veh_type",
                    "veh_cls_res",
                    "timeint",
                    "time",
                    "headway",
                ]
            )
        )
        tt_vissim_headway_fil = tt_vissim_headway.query("~ headway.isna()")

        # Get aggregate statistics for headway across all vissim runs.
        self.tt_vissim_headway_grp = (
            tt_vissim_headway_fil.groupby(
                ["direction", "tt_seg_name", "veh_cls_res", "timeint"]
            )
            .agg(
                avg_headway=("headway", "mean"),
                min_headway=("headway", "min"),
                q50_headway=("headway", lambda x: np.quantile(x, 0.5)),
                q95_headway=("headway", lambda x: np.quantile(x, 0.95)),
                max_headway=("headway", "max"),
                std_dev_headway=("headway", "std"),
                coeff_var_headway=("headway", lambda x: x.std() / x.mean()),
            )
            .dropna(axis=0)
            .reset_index()
        )

    def save_headway(self):
        """
        Save headwy data.
        """
        self.tt_vissim_headway_grp.to_excel(self.path_to_output_headway)


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
        "Private Bus": [305],
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
    # [101, 102, 103, 104, 105, 106]
    keep_tt_segs = (101, 102, 103, 104, 105, 106, 107, 108)

    bus_headway_am = BusHeadway(
        paths_tt_vissim_raw_=paths_tt_vissim_raw,
        path_to_mapper_bus_headway_=path_to_mapper_bus_headway,
        path_to_output_headway_=path_to_output_headway,
    )
    # Read the raw rsr files, filter rows and columns, combine data from different runs
    # and get summary statistics for each simulation run.
    bus_headway_am.read_rsr_tt(
        order_timeint_=order_timeint,
        order_timeint_labels_=order_timeint_labels_am,
        veh_types_res_cls_=veh_types_res_cls,
        keep_cols_=keep_cols,
        keep_tt_segs_=keep_tt_segs,
    )
    # Add travel time segment name and direction to the raw data for each simulation
    # run.
    bus_headway_am.merge_mapper()
    bus_headway_am.get_headway_stats()
    bus_headway_am.save_headway()
