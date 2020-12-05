import pandas as pd
import numpy as np
import os
import glob
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root
import seaborn as sns
import matplotlib.pyplot as plt
import re


class TtEval:
    def __init__(
        self,
        path_to_mapper_tt_seg_,
        paths_tt_vissim_raw_,
        path_output_tt_,
        path_to_output_tt_fig_,
    ):
        """
        Parameters
        ----------
        path_to_mapper_tt_seg_: str
        paths_tt_vissim_raw_: list
            Full path to vissim files from different runs.
        path_output_tt_: str
            Path to output file for processed travel time result.
        path_to_output_tt_fig_: str
            Path to output file for processed travel time figures.
        """
        self.path_to_mapper_tt_seg = path_to_mapper_tt_seg_
        self.paths_tt_vissim_raw = paths_tt_vissim_raw_
        self.path_output_tt = path_output_tt_
        self.path_to_output_tt_fig = path_to_output_tt_fig_
        self.veh_types_res_cls = {}
        self.veh_types_res_cls_df = pd.DataFrame()
        self.tt_mapper = pd.read_excel(path_to_mapper_tt_seg_)
        self.tt_vissim_raw = pd.DataFrame()
        self.tt_vissim_raw_grp_runs = pd.DataFrame()
        self.tt_vissim_raw_grps_ttname_agg = pd.DataFrame()

    def read_rsr_tt(
        self,
        order_timeint_,
        order_timeint_labels_,
        keep_tt_segs_,
        veh_types_res_cls_,
        keep_cols_,
        **kwargs
    ):
        """
        Read the .rsr files from all vissim runs and combine the data into a single
        dataframe.
        Parameters
        ----------
        order_timeint: Order of timeint.
        order_timeint_labels_: Labels for the timeint.
        use_data_col_no_: Data collection points to use.
        keep_tt_segs_: Travel time segments that are relevant for travel time. Some
            travel time segemnts are used for bus headway calculation, thus are not
            used for travel time processing.
        veh_types_res_cls_: dict
            Dictinoary of result vehicles class to vissim vehicle tyeps.
        keep_cols_: Filter columns.
        """
        if keep_cols_ is None:
            keep_cols_ = ["time", "no", "veh", "veh_type", "trav", "delay", "dist"]

        # Create an interval index based on the order_timeint.
        order_timeint_intindex_ = pd.IntervalIndex.from_tuples(
            [
                (int(timeint.split("-")[0]), int(timeint.split("-")[1]))
                for timeint in order_timeint_
            ],
            closed="left",
        )
        timeint_dict = {
            x: y for (x, y) in zip(order_timeint_intindex_, order_timeint_labels_)
        }
        list_tt_vissim_raw_grp_run = []
        list_tt_vissim_raw = []
        self.veh_types_res_cls = veh_types_res_cls_
        # Create a long dataframe from veh_types_res_cls_.
        self.veh_types_res_cls_df = (
            pd.DataFrame.from_dict(veh_types_res_cls_, orient="index")
            .reset_index()
            .melt("index")
            .drop(columns="variable")
            .dropna()
            .rename(columns={"index": "veh_cls_res", "value": "veh_type"})
        )

        for path_tt_vissim_raw in self.paths_tt_vissim_raw:
            tt_vissim_raw = pd.read_csv(path_tt_vissim_raw, sep=";", skiprows=8)
            tt_vissim_raw.columns = remove_special_char_vissim_col(
                tt_vissim_raw.columns
            )
            tt_vissim_raw = (
                tt_vissim_raw.loc[lambda df: df.no.isin(keep_tt_segs_)]
                .filter(items=keep_cols_)
                .assign(
                    timeint=lambda df: pd.cut(df.time, order_timeint_intindex_).map(
                        timeint_dict
                    ),
                    veh_delay=lambda df: df.delay,
                    veh_count=1,
                    dist_ft=lambda df: df.dist * 3.28084,
                    speed=lambda df: df.dist_ft / df.trav / 1.46667,
                )
                .drop(columns=["dist"])
            )
            # TODO : Fix the veh_delay and person_delay piece
            # Person Delay = Vehicle Delay * Occupancy
            tt_vissim_raw = tt_vissim_raw.merge(
                self.veh_types_res_cls_df, on="veh_type", how="left"
            )

            file_nm = os.path.basename(path_tt_vissim_raw)
            file_no = int(file_nm.split(".")[0].split("_")[1])
            tt_vissim_raw.loc[:, "run_no"] = file_no

            tt_vissim_raw_grp_runs = tt_vissim_raw.groupby(
                ["run_no", "timeint", "no", "veh_cls_res"]
            ).agg(
                avg_veh_delay=("veh_delay", "mean"),
                avg_trav=("trav", "mean"),
                q95_trav=("trav", lambda x: np.quantile(x, 0.95)),
                avg_speed=("speed", "mean"),
                avg_dist_ft=("dist_ft", "mean"),
                tot_veh=("veh_count", "sum"),
            )
            if "use_data_col_res" in kwargs:
                if kwargs["use_data_col_res"] == True:
                    paths_data_col_vissim_raw = kwargs["paths_data_col_vissim_raw_"]
                    use_data_col_no = kwargs["use_data_col_no_"]
                    for path in paths_data_col_vissim_raw:
                        file_nm_dat_col = os.path.basename(path)
                        file_no_dat_col = int(
                            file_nm_dat_col.split(".")[0].split("_")[1]
                        )
                        if file_no == file_no_dat_col:
                            row = 0
                            with open(path) as input_file:
                                for current_line in input_file:
                                    first_line = re.compile(r"")
                                    len_ = len(re.split(";", current_line))
                                    if len_ > 5:
                                        break
                                    row = row + 1
                            dat_col_persons = pd.read_csv(path, skiprows=row, sep=";")
                            dat_col_persons.columns = remove_special_char_vissim_col(
                                dat_col_persons.columns
                            )
                            dat_col_persons_fil = (
                                dat_col_persons.loc[
                                    lambda df: (df.measurem.isin(use_data_col_no))
                                    & (df.t_entry > 0)
                                    & (df["vehicle type"] >= 300)
                                ]
                                .rename(
                                    columns={
                                        "veh_no": "veh",
                                        "vehicle type": "veh_type_temp",
                                    }
                                )
                                .sort_values("t_entry")
                                .drop_duplicates("veh")
                                .filter(items=["veh", "veh_type_temp", "pers"])
                            )
                            tt_vissim_raw = tt_vissim_raw.merge(
                                dat_col_persons_fil, on="veh", how="left"
                            )
                            # Assuming all veh type < 300 are not buses.
                            # Assuming all veh type above 300 are busses
                            tt_vissim_raw.loc[
                                lambda df: (df.veh_type < 300), "pers"
                            ] = 1
                            assert not tt_vissim_raw.pers.isna().values.any(), (
                                "Check if there is occupancy data collected in data"
                                "collection point for all buses."
                            )

                            tt_vissim_raw = tt_vissim_raw.assign(
                                pers_delay=lambda df: df.pers * df.veh_delay
                            )

                            tt_vissim_raw_grp_runs_extra = (
                                tt_vissim_raw.groupby(
                                    ["run_no", "timeint", "no", "veh_cls_res"]
                                )
                                .agg(
                                    tot_pers=("pers", "sum"),
                                    tot_pers_delay=("pers_delay", "sum"),
                                )
                                .assign(
                                    avg_pers_delay=lambda df: df.tot_pers_delay
                                    / df.tot_pers
                                )
                            )
                            tt_vissim_raw_grp_runs = pd.merge(
                                tt_vissim_raw_grp_runs,
                                tt_vissim_raw_grp_runs_extra,
                                left_index=True,
                                right_index=True,
                            )
            list_tt_vissim_raw_grp_run.append(tt_vissim_raw_grp_runs)
            list_tt_vissim_raw.append(tt_vissim_raw)

        self.tt_vissim_raw = pd.concat(list_tt_vissim_raw).reset_index()
        self.tt_vissim_raw_grp_runs = pd.concat(
            list_tt_vissim_raw_grp_run
        ).reset_index()


    def merge_mapper(self):
        """
        Add travel time segment names and direction to self.tt_vissim_raw .
        """
        self.tt_mapper = self.tt_mapper.sort_values(
            ["direction", "sort_order"]
        ).reset_index()
        self.tt_vissim_raw = self.tt_vissim_raw.merge(
            self.tt_mapper, left_on="no", right_on="tt_seg_no", how="right"
        ).assign(
            tt_seg_name=lambda df: pd.Categorical(
                df.tt_seg_name, self.tt_mapper.tt_seg_name.values, ordered=True
            ),
            direction=lambda df: pd.Categorical(
                df.direction,
                self.tt_mapper.direction.drop_duplicates().values,
                ordered=True,
            ),
        )

        self.tt_vissim_raw_grp_runs = self.tt_vissim_raw_grp_runs.merge(
            self.tt_mapper, left_on="no", right_on="tt_seg_no", how="right"
        ).assign(
            tt_seg_name=lambda df: pd.Categorical(
                df.tt_seg_name, self.tt_mapper.tt_seg_name.values, ordered=True
            ),
            direction=lambda df: pd.Categorical(
                df.direction,
                self.tt_mapper.direction.drop_duplicates().values,
                ordered=True,
            ),
        )

    def agg_tt(
        self,
        results_cols_=(
            "avg_trav",
            "avg_speed",
            "q95_trav",
            "avg_trav",
            "avg_veh_delay",
        ),
    ):
        """
        Aggregate travel time features. Reformat data to report format.
        """
        agg_dict = {
            "avg_veh_delay": "mean",
            "avg_trav": "mean",
            "q95_trav": "mean",
            "avg_speed": "mean",
            "avg_dist_ft": "mean",
            "tot_veh": "mean",
            "tot_pers": "mean",
            "tot_pers_delay": "mean",
            "avg_pers_delay": "mean",
            "direction": "first"
        }

        agg_dict_filter = dict(
            filter(
                lambda elem: elem[0] in self.tt_vissim_raw_grp_runs.columns,
                agg_dict.items(),
            )
        )

        self.tt_vissim_raw_grps_ttname_agg = (
            self.tt_vissim_raw_grp_runs.groupby(
                ["timeint", "tt_seg_name", "veh_cls_res"]
            )
            .agg(agg_dict_filter)
            .assign(
                avg_speed_from_tt=lambda df: np.round(
                    df.avg_dist_ft / df.avg_trav / 1.47, 2
                ),
            )
            .reset_index()
            .set_index(["timeint", "direction", "tt_seg_name", "veh_cls_res"])
            .filter(items=results_cols_)
            .sort_index()
            .unstack()
            .swaplevel(axis=1)
        )
        mux = pd.MultiIndex.from_product(
            [list(self.veh_types_res_cls.keys()), results_cols_],
            names=["veh_cls_res", ""],
        )
        self.tt_vissim_raw_grps_ttname_agg = self.tt_vissim_raw_grps_ttname_agg.reindex(
            mux, axis=1
        )
        self.tt_vissim_raw_grps_ttname_agg = self.tt_vissim_raw_grps_ttname_agg.round(2)

    def save_tt_processed(self):
        """
        Save the processed travel time data.
        """
        self.tt_vissim_raw_grps_ttname_agg.to_excel(self.path_output_tt)

    def plot_heatmaps(self, segs_to_plot, var="avg_speed_from_tt"):
        """
        Plot speed data.
        """
        plot_df = (
            self.tt_vissim_raw_grps_ttname_agg.swaplevel(axis=1)
            .stack()
            .reset_index()
            .filter(items=["veh_cls_res", "direction", "tt_seg_name", "timeint", var])
        )
        df_keep_segs = (self.tt_mapper[["tt_seg_no", "tt_seg_name"]]
        .loc[lambda df: df.tt_seg_no.isin(segs_to_plot)])
        plot_df_fil = pd.merge(
            plot_df,
            df_keep_segs,
            on="tt_seg_name",
            how="inner"
        )
        plot_df_grp = plot_df_fil.groupby(["veh_cls_res", "direction"])
        sns.set(font_scale=1)
        for name, group in plot_df_grp:
            plot_df_grp_fil = pd.pivot_table(
                group, values=var, columns="timeint", index="tt_seg_name"
            )
            plot_df_grp_fil = plot_df_grp_fil.sort_index(ascending=False)
            color_bar_ = "viridis"
            fig, ax = plt.subplots(1, figsize=(8, 3))
            g = sns.heatmap(
                plot_df_grp_fil,
                vmin=0,
                vmax=70,
                annot=True,
                cmap=color_bar_,
                linewidths=0.5,
                ax=ax,
                square=False,
                fmt=".1f",
            )

            g.set_xticklabels(rotation=30, labels=g.get_xticklabels(), ha="right")
            g.set_yticklabels(rotation=30, labels=g.get_yticklabels())
            g.set_ylabel("")
            g.set_xlabel("Time Interval")
            path_to_output_tt_fig_filenm = os.path.join(
                self.path_to_output_tt_fig, "_".join([name[0], name[1], ".jpg"])
            )
            fig.savefig(path_to_output_tt_fig_filenm, bbox_inches="tight")
            plt.close()
            print(name)


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
        "avg_speed_from_tt",
        "avg_dist_ft",
    ]
    # Result travel time segment number to be retained in the output.
    # [1,2,3,4,5,6,7,8,9,10,11,12]
    keep_tt_segs = [1, 23, 4, 20, 24, 21, 11, 12, 13, 25]

    # Which Travel time segments to include in travel time results
    plot_tt_segs = [1, 23, 4, 20, 21, 11, 12, 13]
    tt_eval_am = TtEval(
        path_to_mapper_tt_seg_=path_to_mapper_tt_seg,
        paths_tt_vissim_raw_=paths_tt_vissim_raw,
        path_output_tt_=path_to_output_tt,
        path_to_output_tt_fig_=path_to_output_tt_fig,
    )
    # Read the raw rsr files, filter rows and columns, combine data from different runs
    # and get summary statistics for each simulation run.
    tt_eval_am.read_rsr_tt(
        order_timeint_=order_timeint,
        order_timeint_labels_=order_timeint_labels_am,
        veh_types_res_cls_=veh_types_res_cls,
        keep_cols_=keep_cols,
        keep_tt_segs_=keep_tt_segs,
        paths_data_col_vissim_raw_=paths_data_col_vissim_raw,
        use_data_col_no_=use_data_col_no,
        use_data_col_res=True,
    )
    # Add travel time segment name and direction to the data with summary statistics for
    # each simulation run.
    tt_eval_am.merge_mapper()
    # Aggregate travel time results to get an average of all simulation runs.
    tt_eval_am.agg_tt(results_cols_=results_cols)
    tt_eval_am.save_tt_processed()
    tt_eval_am.plot_heatmaps(segs_to_plot = plot_tt_segs,
                             var="avg_speed_from_tt")
