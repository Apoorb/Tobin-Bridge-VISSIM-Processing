import pandas as pd
import os
from pathlib import Path
from tobin_process.utils import get_project_root
import inflection
import numpy as np


def LOS_Calc(Delay):
    LOS = ""
    if Delay <= 10:
        LOS = "A"
    elif (Delay > 10) & (Delay <= 20):
        LOS = "B"
    elif (Delay > 20) & (Delay <= 35):
        LOS = "C"
    elif (Delay > 35) & (Delay <= 55):
        LOS = "D"
    elif (Delay > 55) & (Delay <= 80):
        LOS = "E"
    elif Delay > 80:
        LOS = "F"
    else:
        LOS = ""
    return LOS


def LOS_Calc_TWSC(Delay):
    LOS = ""
    if Delay <= 10:
        LOS = "A"
    elif (Delay > 10) & (Delay <= 15):
        LOS = "B"
    elif (Delay > 15) & (Delay <= 25):
        LOS = "C"
    elif (Delay > 25) & (Delay <= 35):
        LOS = "D"
    elif (Delay > 35) & (Delay <= 50):
        LOS = "E"
    elif Delay > 50:
        LOS = "F"
    else:
        LOS = ""
    return LOS


if __name__ == "__main__":
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")

    path_to_mappers_data = os.path.join(path_to_prj, "data", "mappers")
    path_to_mapper_node_eval = os.path.join(
        path_to_mappers_data, "node_evaluation_vissim_report_mapping.xlsx"
    )

    path_to_node_eval_res = os.path.join(
        path_to_raw_data, "Tobin Bridge Base Model_Node Results.att"
    )
    node_eval_mapper = pd.read_excel(
        path_to_mapper_node_eval, sheet_name="vissim_report_convertion"
    )
    node_eval_deduplicate = pd.read_excel(
        path_to_mapper_node_eval, sheet_name="deduplicate_movements"
    )
    node_eval_res = pd.read_csv(path_to_node_eval_res, comment="*", sep=";", skiprows=1)

    node_eval_res.columns = [
        inflection.underscore(colnm)
        .replace("$", "")
        .replace(":", "_")
        .replace("\\", "_")
        .replace("(", "_")
        .replace(")", "")
        for colnm in node_eval_res.columns
    ]

    node_eval_res_avg = (
        node_eval_res.loc[
            lambda df: (
                (df.movementevaluation_simrun == "AVG")
                & (df.movement_fromlink_level.isin([1, np.nan]))
            )
        ]
        .filter(
            items=[
                "movementevaluation_simrun",
                "timeint",
                "movement",
                "movement_direction",
                "movement_fromlink_level",
                "qlen",
                "qlenmax",
                "vehs_all",
                "vehdelay_all",
            ]
        )
        .assign(
            node_no=lambda df: df.movement.str.extract("(\d*).*?").astype(int),
            from_link=lambda df: df.movement.str.extract(
                "(?:[^:]*)?(?::\W?)?([^@]*)?"
            ).transform(lambda x: x.str.strip()),
            to_link=lambda df: df.movement.str.extract(
                "(?:[^@]*)?(?:[^:]*)?(?::\W?)?([^@]*)?"
            ).transform(lambda x: x.str.strip()),
        )
    )

    node_eval_deduplicate = node_eval_deduplicate.assign(
        from_link=lambda df: df.from_link.str.strip(),
        to_link=lambda df: df.to_link.str.strip(),
        movement_direction=lambda df: df.movement_direction.str.strip(),
    )

    node_eval_deduplicate_test = node_eval_deduplicate.merge(
        node_eval_res_avg.filter(
            items=["node_no", "movement_direction", "from_link", "to_link"]
        )
        .drop_duplicates(["node_no", "movement_direction", "from_link", "to_link"])
        .assign(allpresent=True),
        on=["node_no", "movement_direction", "from_link", "to_link"],
        how="left",
    ).assign(allpresent=lambda df: df.allpresent.fillna(False))

    try:
        assert node_eval_deduplicate_test.allpresent.all(), (
            "Some rows did not match " "between Vissim and Mapper"
        )
    except AssertionError as err:
        print(err)
        print(
            "movement_direction, to_link, or/ and from_link does not match between "
            "Vissim and the mapper for the following:"
        )
        with pd.option_context("display.max_columns", 10):
            print(node_eval_deduplicate_test.loc[lambda df: ~df.allpresent])

    node_eval_res_avg_uniq_dir = (
        node_eval_res_avg.merge(
            node_eval_deduplicate,
            on=["node_no", "movement_direction", "from_link", "to_link"],
            how="left",
        )
        .assign(
            movement_direction_unique=(
                lambda df: df.movement_direction_unique.fillna(df.movement_direction)
            )
        )
        .drop(columns="movement_direction")
    )

    order_direction_results = [
        "NBR",
        "NBT",
        "NBL",
        "NB",
        "NER",
        "NET (US-1 and I-93)",
        "NET (MA 3)",
        "NET",
        "NEL",
        "NE" "EBR",
        "EBT",
        "EBL",
        "EB",
        "SER",
        "SET",
        "SET (Martha Road)",
        "SET (US-1 and I-93)",
        "SET (MA 3)",
        "SEL",
        "SE",
        "SBR",
        "SBT",
        "SBL",
        "SBL (Martha Road)",
        "SBL (MA 3)",
        "SB",
        "SWR",
        "SWT",
        "SWL",
        "SW",
        "WBR",
        "WBT",
        "WBL",
        "WB",
        "NWR",
        "NWR (US-1 and I-93)",
        "NWR (MA 3)",
        "NWT",
        "NWL (MA 3)",
        "NWL (US-1 and I-93)",
        "NWL",
        "NW",
        "Intersection",
    ]

    node_eval_res_avg_uniq_dir_report_dir = (
        node_eval_res_avg_uniq_dir.merge(
            node_eval_mapper.assign(
                movement_direction_unique=lambda df: df.movement_direction_unique.str.strip()
            ),
            on=["node_no", "movement_direction_unique"],
            how="left",
        )
        .loc[lambda df: df.movement_direction_unique != "Total"]
        .assign(main_dir=lambda df: df.direction_results.str.extract("(\S{2})"),)
    )

    node_eval_intersection = node_eval_res_avg_uniq_dir_report_dir.filter(
        items=["node_no", "timeint", "vehs_all", "vehdelay_all"]
    ).assign(
        tot_intersection_veh=lambda df: df.groupby(
            ["node_no", "timeint"]
        ).vehs_all.transform(sum),
        veh_into_veh_delay=lambda df: df.vehs_all
        * df.vehdelay_all
        / df.tot_intersection_veh,
    )
    node_eval_intersection_grp = (
        node_eval_intersection.groupby(["node_no", "timeint"])
        .agg(vehdelay_all=("veh_into_veh_delay", sum))
        .reset_index()
        .assign(direction_results="Intersection")
    )

    node_eval_res_avg_uniq_dir_report_dir_int = pd.concat(
        [node_eval_res_avg_uniq_dir_report_dir, node_eval_intersection_grp]
    )
    order_timeint = ["900-4500", "4500-8100", "8100-11700", "11700-12600"]
    node_eval_res_avg_uniq_dir_report_dir_int_pivot = (
        node_eval_res_avg_uniq_dir_report_dir_int.assign(
            timeint=lambda df: pd.Categorical(df.timeint, order_timeint),
            direction_results=lambda df: pd.Categorical(
                df.direction_results, order_direction_results
            ),
        )
        .sort_values(["timeint", "node_no", "direction_results"])
        .reset_index(drop=True)
        .filter(
            items=[
                "node_no",
                "main_dir",
                "direction_results",
                "timeint",
                "qlen",
                "from_link",
                "to_link",
                "qlenmax",
                "vehs_all",
                "vehdelay_all",
            ]
        )
        .set_index(
            [
                "node_no",
                "main_dir",
                "direction_results",
                "from_link",
                "to_link",
                "timeint",
            ]
        )
        .unstack()
        .swaplevel(axis=1)
        .sort_index()
    )

    mux = pd.MultiIndex.from_product(
        [
            ["900-4500", "4500-8100", "8100-11700", "11700-12600"],
            ["vehdelay_all", "vehs_all", "qlenmax", "qlen"],
        ],
        names=["timeint", ""],
    )
    node_eval_res_avg_uniq_dir_report_dir_int_pivot = node_eval_res_avg_uniq_dir_report_dir_int_pivot.reindex(
        mux, axis=1
    )

    path_to_output_node_data = os.path.join(
        path_to_interim_data, "process_node_eval.xlsx"
    )
    node_eval_res_avg_uniq_dir_report_dir_int_pivot.to_excel(path_to_output_node_data)


