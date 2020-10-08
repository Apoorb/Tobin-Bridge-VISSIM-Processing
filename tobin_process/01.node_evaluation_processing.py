import pandas as pd
import os
from pathlib import Path
from tobin_process.utils import get_project_root
import tobin_process.node_evaluation_helper as node_eval_helper  # noqa E402
import inflection
import numpy as np

if __name__ == "__main__":
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")
    path_to_mappers_data = os.path.join(path_to_prj, "data", "mappers")
    path_to_mapper_node_eval = os.path.join(
        path_to_mappers_data, "node_evaluation_vissim_report_mapping.xlsx"
    )
    path_to_node_eval_res_am = os.path.join(
        path_to_raw_data, "Tobin Bridge Base Model_Node Results.att"
    )
    path_to_output_node_data = os.path.join(
        path_to_interim_data, "process_node_eval.xlsx"
    )

    keep_cols = [
        "$MOVEMENTEVALUATION:SIMRUN",  # would need for all projects
        "TIMEINT",  # would need for all projects
        "MOVEMENT",  # would need for all projects
        "MOVEMENT\DIRECTION",  # would need for all projects
        "MOVEMENT\FROMLINK\LEVEL",  # would need for all projects
        "QLEN",
        "QLENMAX",
        "VEHS(ALL)",
        "VEHDELAY(ALL)",
    ]

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
        "NE",
        "EBR",
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

    order_timeint = ["900-4500", "4500-8100", "8100-11700", "11700-12600"]

    node_eval_am = node_eval_helper.NodeEval(
        path_to_prj_=path_to_prj,
        path_to_raw_data_=path_to_raw_data,
        path_to_interim_data_=path_to_interim_data,
        path_to_mapper_node_eval_=path_to_mapper_node_eval,
        path_to_node_eval_res_=path_to_node_eval_res_am,
        path_to_output_node_data_=path_to_output_node_data,
        remove_duplicate_dir=True,
    )

    node_eval_am.clean_node_eval(
        keep_cols_=keep_cols,
        keep_runs_=["AVG"],
        keep_movement_fromlink_level_=[1, np.nan],
    )

    node_eval_am.node_eval_res_fil.head()

    node_eval_am.test_deduplicate_has_correct_values()

    node_eval_am.test_unique_dir_per_node()

    node_eval_am.get_veh_delay_by_intersection()

    node_eval_am.get_veh_delay_by_approach()

    node_eval_am.set_report_data(
        df_list=[
            node_eval_am.node_eval_res_fil_uniq_dir,
            node_eval_am.node_intersection_delay,
            node_eval_am.node_approach_delay,
        ]
    )

    node_eval_am.set_los()

    non_result_columns = [
        "movementevaluation_simrun",
        "timeint",
        "movement",
        "movement_fromlink_level",
        "node_no",
        "from_link",
        "to_link",
        "movement_direction_unique",
        "direction_results",
        "node_type",
        "main_dir",
    ]

    result_cols = set(node_eval_am.report_data.columns) - set(non_result_columns)
    # {'los', 'qlen', 'qlenmax', 'vehdelay_all', 'vehs_all'}

    print(f"Choose result columns from the following: {result_cols}")
    results_cols = ["qlen", "qlenmax", "vehs_all", "vehdelay_all", "los"]

    node_eval_am.format_report_table(
        order_direction_results_=order_direction_results,
        order_timeint_=order_timeint,
        results_cols_=results_cols,
    )

    node_eval_am.save_output_file()
