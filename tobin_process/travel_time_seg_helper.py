import pandas as pd
import numpy as np
import os
import glob
import inflection
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root


class TtEval:
    def __init__(
        self,
        path_to_prj_,
        path_to_raw_data_,
        path_to_interim_data_,
        path_to_mapper_tt_seg_,
        paths_tt_vissim_raw_,
        path_output_tt_,
    ):
        self.path_to_prj = path_to_prj_
        self.path_to_raw_data = path_to_raw_data_
        self.path_to_interim_data = path_to_interim_data_
        self.path_to_mapper_tt_seg = path_to_mapper_tt_seg_
        self.paths_tt_vissim_raw = paths_tt_vissim_raw_
        self.path_output_tt = path_output_tt_
        self.veh_types_res_cls = {}
        self.veh_types_res_cls_df = pd.DataFrame()
        self.tt_mapper = pd.read_excel(path_to_mapper_tt_seg_)
        self.tt_vissim_raw_grps = pd.DataFrame()
        self.tt_vissim_raw_grps_ttname = pd.DataFrame()
        self.tt_vissim_raw_grps_ttname_agg = pd.DataFrame()

    def read_rsr_tt(
        self,
        order_timeint_intindex_,
        order_timeint_labels_,
        veh_types_occupancy_,
        veh_types_res_cls_={100: "car_hgv", 200: "car_hgv", 300: "bus"},
        keep_cols_=["time", "no", "veh", "veh_type", "trav", "delay", "dist"],
    ):
        timeint_dict={x: y for (x, y) in zip(order_timeint_intindex_, order_timeint_labels_)}
        list_tt_vissim_raw_grp = []
        self.veh_types_res_cls = veh_types_res_cls_
        self.veh_types_res_cls_df = (
            pd.DataFrame.from_dict(
                veh_types_res_cls_,
                orient="index"
            )
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
                tt_vissim_raw.filter(items=keep_cols_)
                .assign(
                    timeint=lambda df: pd.cut(df.time,
                                              order_timeint_intindex_).map(timeint_dict),
                    veh_count = 1,
                    veh_type_tot = (
                        lambda df:
                        df
                        .groupby(["timeint", "veh_type", "no"])
                        .veh_count
                        .transform(sum)
                    ),
                    veh_type_occupancy=lambda df: df.veh_type.replace(veh_types_occupancy_),
                    veh_type_occupancy_tol = (
                        lambda df:
                        df
                        .groupby(["timeint", "veh_type", "no"])
                        .veh_type_occupancy
                        .transform(sum)
                    ),
                    veh_delay = lambda df: df.delay * df.veh_count / df.veh_type_tot,
                    person_delay = lambda df: df.delay * df.veh_type_occupancy / df.veh_type_occupancy_tol,
                    dist_ft=lambda df: df.dist * 3.28084,
                    speed=lambda df: df.dist_ft / df.trav / 1.46667,
                )
                .drop(columns=["dist"])
            )

            tt_vissim_raw = tt_vissim_raw.merge(
                self.veh_types_res_cls_df,
                on="veh_type",
                how="left"
            )

            tt_vissim_raw_grp = (
                tt_vissim_raw.groupby(["timeint", "veh_cls_res", "no"])
                .agg(
                    avg_veh_delay = ("veh_delay", "mean"),
                    avg_person_delay=("person_delay", "mean"),
                    tot_veh=("veh_count", "sum"),
                    tot_people=("veh_type_occupancy", "sum"),
                    avg_trav=("trav", "mean"),
                    q95_trav=("trav", lambda x: np.quantile(x, 0.95)),
                    avg_speed=("speed", "mean"),
                )
                .reset_index()
            )

            list_tt_vissim_raw_grp.append(tt_vissim_raw_grp)
        self.tt_vissim_raw_grps = pd.concat(list_tt_vissim_raw_grp).reset_index()

    def merge_mapper(self):
        self.tt_vissim_raw_grps_ttname = self.tt_vissim_raw_grps.merge(
            self.tt_mapper, left_on="no", right_on="tt_seg_no", how="right"
        )

    def agg_tt(self, results_cols_=["avg_trav", "speed", "q95_trav", "avg_trav", "avg_veh_delay", "avg_person_delay"]):
        self.tt_vissim_raw_grps_ttname_agg = (
            self.tt_vissim_raw_grps_ttname.groupby(
                ["timeint", "tt_seg_name", "veh_cls_res"]
            )
            .agg(
                tot_veh=("tot_veh", "mean"),
                tot_people=("tot_people", "mean"),
                avg_veh_delay=("avg_veh_delay", "mean"),
                avg_person_delay=("avg_person_delay", "mean"),
                avg_trav=("avg_trav", "mean"),
                q95_trav=("q95_trav", "mean"),
                avg_speed=("avg_speed", "mean"),
                direction=("direction", "first"),
            )
            .reset_index()
            .assign(
                tt_seg_name=lambda df: pd.Categorical(
                    df.tt_seg_name, self.tt_mapper.tt_seg_name.values, ordered=True
                ),
                direction=lambda df: pd.Categorical(
                    df.direction,
                    self.tt_mapper.direction.drop_duplicates().values,
                    ordered=True,
                ),
                tot_veh=lambda df: df.tot_veh.round(2),
                tot_people=lambda df: df.tot_people.round(2),
                avg_trav=lambda df: df.avg_trav.round(2),
                q95_trav= lambda df: df.q95_trav.round(2),
                avg_speed = lambda df: df.avg_speed.round(2),
                avg_delay=lambda df: df.avg_veh_delay.round(2),
                avg_person_delay=lambda df: df.avg_person_delay.round(2),
                avg_veh_delay=lambda df: df.avg_veh_delay.round(2)
            )
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

    def save_tt_processed(self):
        self.tt_vissim_raw_grps_ttname_agg.to_excel(self.path_output_tt)


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
    order_timeint_labels = order_timeint
    order_timeint_intindex = pd.IntervalIndex.from_tuples(
        [
            (int(timeint.split("-")[0]), int(timeint.split("-")[1]))
            for timeint in order_timeint
        ],
        closed="left",
    )

    veh_types_res_cls = {
        "car" : [100],
        "hgv" : [200],
        "car_hgv": [100, 200],
        "bus" : [300],
        "car_hgv_bus": [100, 200, 300],
    }

    veh_types_occupancy = {100:1, 200:1, 300:60}
    keep_cols = ["time", "no", "veh", "veh_type", "trav", "delay", "dist"]
    results_cols = ["avg_trav", "avg_speed", "q95_trav", "avg_veh_delay",
                     "avg_person_delay", "tot_veh", "tot_people"]

    tt_eval_am = TtEval(
        path_to_prj_=path_to_prj,
        path_to_raw_data_=path_to_raw_data,
        path_to_interim_data_=path_to_interim_data,
        path_to_mapper_tt_seg_=path_to_mapper_tt_seg,
        paths_tt_vissim_raw_=paths_tt_vissim_raw,
        path_output_tt_=path_to_output_tt,
    )

    tt_eval_am.read_rsr_tt(
        order_timeint_intindex_=order_timeint_intindex,
        order_timeint_labels_ = order_timeint_labels,
        veh_types_occupancy_ = veh_types_occupancy,
        veh_types_res_cls_=veh_types_res_cls,
        keep_cols_=keep_cols,
    )
    tt_eval_am.merge_mapper()
    tt_eval_am.agg_tt(results_cols_=results_cols)
    tt_eval_am.save_tt_processed()
