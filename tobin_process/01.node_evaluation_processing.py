"""
Script for processing node evaluation results for Tobin Bridge. The key element of this
script is the mapper file with vissim--->report direction mapping and vissim--->report
direction mapping based on to and from link names. Can re-tool this script to work on
other Vissim projects also.
"""
import os
from tobin_process.utils import get_project_root
import tobin_process.node_evaluation_helper as node_eval_helper  # noqa E402
import numpy as np

if __name__ == "__main__":
    # 1. Set the paths for input files and output files.
    # ************************************************************************************
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")
    path_to_mappers_data = os.path.join(path_to_prj, "data", "mappers")
    path_to_mapper_node_eval = os.path.join(
        path_to_mappers_data, "node_evaluation_vissim_report_mapping.xlsx"
    )
    path_to_node_eval_res_am = os.path.join(
        path_to_raw_data, "Tobin Bridge Base Model - AM Peak Period V3_Node Results.att"
    )
    path_to_output_node_data = os.path.join(
        path_to_interim_data, "process_node_eval.xlsx"
    )

    # 2. Set columns to keep, direction order, time interval order, columns to include
    # in results.
    # ************************************************************************************
    # Columns required for result processing.
    # All of these columns would not be present in VISSIM node evaluation output file
    # by default. Check, if you have included these columns in the VISSIM output file.
    keep_cols = [
        "$MOVEMENTEVALUATION:SIMRUN",  # would need for all projects
        "TIMEINT",  # would need for all projects
        "MOVEMENT",  # would need for all projects
        r"MOVEMENT\DIRECTION",  # would need for all projects
        r"MOVEMENT\FROMLINK\LEVEL",  # would need for all projects
        "QLEN",
        "QLENMAX",
        "VEHS(ALL)",
        "VEHDELAY(ALL)",
    ]
    # Sort order for the report directions.
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
    # Sort order for the report time interval.
    order_timeint = ["2700-6300", "6300-9900", "9900-13500", "13500-14400"]
    # Labels for different time intervals.
    order_timeint_labels_am = {
        "2700-6300": "6:00-7:00 am",
        "6300-9900": "7:00-8:00 am",
        "9900-13500": "8:00-9:00 am",
        "13500-14400": "9:00-9:15 am",
    }
    # Sort order for the report results column.
    results_cols = ["qlen", "qlenmax", "vehdelay_all", "los"]
    # Initialize NodeEval class.
    # Check your mapper (path_to_mapper_node_eval) file and the VISSIM network to ensure
    # the mapping between Vissim direction and cardinal directions is correct. Also,
    # check tab in the mapper file for de-duplicating direction and ensure that the
    # movements for which de-duplication needs to take place have to_link and from_link
    # names.
    node_eval_am = node_eval_helper.NodeEval(
        path_to_mapper_node_eval_=path_to_mapper_node_eval,
        path_to_node_eval_res_=path_to_node_eval_res_am,
        path_to_output_node_data_=path_to_output_node_data,
        remove_duplicate_dir=True,
    )
    # After the execution of this function, analyst can access:
    #   node_eval_am.node_eval_res, node_eval_am.node_eval_mapper,
    #   node_eval_am.node_no_node_type, node_eval_am.node_eval_deduplicate.
    # Check node_eval_helper documentation for more details.

    # Filter rows and columns of the raw vissim node evaluation data.
    # Assign unique directions to all movements.
    # Check if you actually have the results for the run you are trying to evaluate.
    # After the execution of this function, analyst can access:
    #   node_eval_am.node_eval_res_fil
    node_eval_am.clean_node_eval(
        keep_cols_=keep_cols,
        keep_runs_=["AVG"],
        keep_movement_fromlink_level_=[1, np.nan],
    )
    # Test if there are missing Vissim directions in the Mapper File
    add_following_direction_to_mapper = node_eval_am.node_eval_res_fil_uniq_dir.query(
        """
            direction_results.isna()
            """
    ).drop_duplicates(["node_no", "direction_results"])
    assert len(add_following_direction_to_mapper) == 0, (
        "Add the above missing rows to " "mapper."
    )
    # Print the filtered vissim node evaluation data.
    node_eval_am.node_eval_res_fil.head()
    # Test if "node_evaluation_vissim_report_mapping.xlsx" , "deduplicate_movements" has
    # same values as defined in VISSIM; doesn't have typos, space etc.
    node_eval_am.test_deduplicate_has_correct_values()
    # Test that each direction in a node occur only one time.
    node_eval_am.test_unique_dir_per_node()
    # Get delay by intersection
    node_eval_am.get_veh_delay_by_intersection()
    # Get delay by approach.
    node_eval_am.get_veh_delay_by_approach()
    # Concatenate data by direction, approach, and intersection.
    node_eval_am.set_report_data(
        df_list=[
            node_eval_am.node_eval_res_fil_uniq_dir,
            node_eval_am.node_intersection_delay,
            node_eval_am.node_approach_delay,
        ]
    )
    # Get LOS based on the type of intersection.
    node_eval_am.set_los()
    # Format report table using multi-index.
    node_eval_am.format_report_table(
        order_direction_results_=order_direction_results,
        order_timeint_=order_timeint,
        results_cols_=results_cols,
        order_timeint_label_=order_timeint_labels_am,
    )
    node_eval_am.save_output_file()
