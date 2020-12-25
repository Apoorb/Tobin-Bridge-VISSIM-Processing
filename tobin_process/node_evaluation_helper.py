"""
Module to process node evaluation results from Tobin Bridge Project.
"""
import pandas as pd
import numpy as np
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root
import os


def los_calc_signal(delay):
    """
    Get the LOS based on delay using HCM 6th Ed methods for signalized intersections.
    """
    los = ""
    if delay <= 10:
        los = "A"
    elif (delay > 10) & (delay <= 20):
        los = "B"
    elif (delay > 20) & (delay <= 35):
        los = "C"
    elif (delay > 35) & (delay <= 55):
        los = "D"
    elif (delay > 55) & (delay <= 80):
        los = "E"
    elif delay > 80:
        los = "F"
    else:
        los = ""
    return los


def los_calc_twsc(delay):
    """
    Get the LOS based on delay using HCM 6th Ed methods for twsc/ awsc intersections.
    """
    los = ""
    if delay <= 10:
        los = "A"
    elif (delay > 10) & (delay <= 15):
        los = "B"
    elif (delay > 15) & (delay <= 25):
        los = "C"
    elif (delay > 25) & (delay <= 35):
        los = "D"
    elif (delay > 35) & (delay <= 50):
        los = "E"
    elif delay > 50:
        los = "F"
    else:
        los = ""
    return los


class NodeEval:
    """ Class for processing node evaluation results from Tobin Bridge Project.

    ...
    Attributes
    -----------
    path_to_mapper_node_eval: str
            Path to the node mapper file that maps vissim direction
    path_to_node_eval_res: str
            Path to the raw vissim data for node evaluation.
    path_to_output_node_data: str
            Path to output file for storing node evaluation results.
    node_eval_mapper: pd.DataFrame()
        Mapper data for converting Vissim directions into traffic operation directions.
    node_no_node_type: pd.DataFrame()
         Mapper data for mapping between node number and node type: signalized or twsc.
    node_eval_deduplicate: pd.DataFrame()
        Data for mapping between to_link and from_link with the movement name. Used for
        de-duplicating turning movements with multiple from or to links. Link names need
        to be added in VISSIM for links passing under node evaluation lines for this data
        to work.
    node_eval_res: pd.DataFrame()
        Vissim node evaluation data with no special charaters for the column names
    node_eval_res_fil: pd.DataFrame()
        Filtered node_eval_res data w.r.t columns, runs, and movement from_link level.
    node_intersection_delay: pd.DataFrame()
        Data aggregated at intersection level.
    node_approach_delay: pd.DataFrame()
        Data aggregated by approach.
    report_data: pd.DataFrame()
        Concatenated turning movement, approach, and intersection data.
    report_data_fil_pivot: pd.DataFrame()
        Pivot the report_data before saving to file.
    keep_cols_cor_nm: list
        Columns to keep in the data.
    keep_runs: list
        Runs to keep in the final output.

    Methods
    -----------
    read_node_eval(): Read vissim node evaluation data. Remove special charaters for the
        column names. Read the data saved in self.path_to_node_eval_res.
    clean_node_eval(keep_cols_, keep_runs_, keep_movement_fromlink_level_): Test if the
        run for which results are needed is actually present in the results. If the run
        is present, then call filter_to_relevant_cols_rows to filter columns using
        keeps_cols, runs using keep_runs, and movement using
        keep_movement_fromlink_level_.
    test_run_present(): Test if keep_runs_ is present in data.
    filter_to_relevant_cols_rows(keep_movement_fromlink_level_): Filter node_eval_res
        on keep_cols_, keep_runs_, keep_movement_fromlink_level_. Save filtered data to
        node_eval_res_fil.
    add_report_directions(): Add cardinal direction to the node_eval_res_fil data based on
     the node_eval_mapper and node_eval_deduplicate data. Save the new data to
     node_eval_res_fil_uniq_dir.
    test_deduplicate_has_correct_values(): Test if the to_links and from_links defined
        in node_eval_deduplicate are present in the vissim data (node_eval_res_fil).
    deduplicate_repeated_dirs(): De-duplicate directions in node_eval_res_fil_uniq_dir
        using the node_eval_deduplicate data.
    test_unique_dir_per_node(): Test if we have unique directions for each node i.e. we
        we successfully renamed dupicated directions using the deduplicate_repeated_dirs()
        function.
    get_veh_delay_by_intersection(): Aggregate delay by intersection.
    get_veh_delay_by_approach(): Aggregate delay by approach.
    set_report_data(df_list): Concat results by direction, approach, and intersection.
    set_los(): Set LOS based on delay.
    format_report_table(
        order_direction_results_,
        order_timeint_,
        results_cols_,
        order_timeint_label_,
    ): Label time intervals, filter results column, set directions in correct sort order.
    save_output_file(): Save the final data.
    """

    def __init__(
        self,
        path_to_mapper_node_eval_,
        path_to_node_eval_res_,
        path_to_output_node_data_,
        remove_duplicate_dir=False,
    ):
        """
        Initialize the class with path to the mapper file that provides a cross-walk
        between the Vissim and the report (cardinal) directions, node evaluation results
        file, path where the output/ processed data should be saved, and if there is a
        need to remove duplicated directions for the an approach.
        Parameters
        ----------
        path_to_mapper_node_eval_: str
            Path to the node mapper file that maps vissim direction
        to report directions.
        path_to_node_eval_res_: str
            Path to the raw vissim data for node evaluation.
        path_to_output_node_data_: str
            Path to output file for storing node evaluation results.
        remove_duplicate_dir: bool
            If True, use vissim_report_convertion sheet in path_to_mapper_node_eval_ to
            deduplicate duplicated directions for same node.
        """
        # Set paths.
        self.path_to_mapper_node_eval = path_to_mapper_node_eval_
        self.path_to_node_eval_res = path_to_node_eval_res_
        self.path_to_output_node_data = path_to_output_node_data_
        # Mapper files for converting Vissim directions into traffic operation directions.
        self.node_eval_mapper = pd.read_excel(
            path_to_mapper_node_eval_, sheet_name="vissim_report_convertion"
        )
        # Read the mapping between node number and node type: signalized or twsc
        self.node_no_node_type = (
            self.node_eval_mapper.filter(items=["node_no", "node_type"])
            .drop_duplicates(["node_no"])
            .reset_index(drop=True)
        )
        self.node_no_node_type = self.node_no_node_type.set_index("node_no")[
            "node_type"
        ].to_dict()
        # Handle case when one direction for a node occurs more than once. For instance,
        # two NBR for a direction need to separated using to and from link names.
        if remove_duplicate_dir:
            self.node_eval_deduplicate = pd.read_excel(
                self.path_to_mapper_node_eval, sheet_name="deduplicate_movements"
            )
            self.node_eval_deduplicate = self.node_eval_deduplicate.assign(
                from_link=lambda df: df.from_link.str.strip(),
                to_link=lambda df: df.to_link.str.strip(),
                movement_direction=lambda df: df.movement_direction.str.strip(),
            )
        else:
            self.node_eval_deduplicate = None
        self.node_eval_res = self.read_node_eval()
        self.node_eval_res_fil = pd.DataFrame()
        self.node_eval_res_fil_uniq_dir = pd.DataFrame()
        self.node_intersection_delay = pd.DataFrame()
        self.node_approach_delay = pd.DataFrame()
        self.report_data = pd.DataFrame()
        self.report_data_fil_pivot = pd.DataFrame()
        self.keep_cols_cor_nm = [
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
        self.keep_runs = ["AVG"]
        print(
            f"Reassigned column names are as follows: "
            f"{self.node_eval_res.columns.values}"
        )
        print(f"By default we keep the following columns: {self.keep_cols_cor_nm}")
        print("Update the list of columns to keep by using keep_cols_cor_nm variable")

    def read_node_eval(self):
        """
        Read vissim node evaluation data. Remove special charaters for the column names
        Read the data saved in self.path_to_node_eval_res.
        """
        # * is comment line. $ also means comment, but in pandas we can only use one
        # char for denoting comment, so using skiprow=1 to skip the 1st row, which has
        # a $ sign. 2nd and last $ sign is used with column name. Will address it below.
        node_eval_res = pd.read_csv(
            self.path_to_node_eval_res, comment="*", sep=";", skiprows=1
        )
        # Remove special charaters from the Vissim names.
        node_eval_res.columns = remove_special_char_vissim_col(node_eval_res.columns)
        return node_eval_res

    def clean_node_eval(self, keep_cols_, keep_runs_, keep_movement_fromlink_level_):
        """
        Test if the run for which results are needed is actually present in the results.
        If the run is present, then call filter_to_relevant_cols_rows to filter
        columns using keeps_cols, runs using keep_runs, and movement using
        keep_movement_fromlink_level_.
        Parameters
        ----------
        keep_cols_: list
            Columns to keep.
        keep_runs_: list
            Runs to process. Generaly would only be interested in average results.
        keep_movement_fromlink_level_: list
            fromlink levels that should be processed. For Tobin bridge, we are only
            interested in level 1 and np.nan (no level data)
        Returns
        -------
        """
        keep_cols_cor_nm = remove_special_char_vissim_col(keep_cols_)
        self.keep_cols_cor_nm = keep_cols_cor_nm
        if type(keep_runs_) == str:
            self.keep_runs = [keep_runs_]
        else:
            self.keep_runs = [str(i) for i in keep_runs_]
        self.test_run_present()
        # Filter to relevant columns and simulation runs (e.g. 1, 2, "AVG")
        self.filter_to_relevant_cols_rows(keep_movement_fromlink_level_)
        self.add_report_directions()

    def test_run_present(self):
        assert self.node_eval_res.movementevaluation_simrun.isin(
            self.keep_runs
        ).any(), f"No data found for run {self.keep_runs}"

    def filter_to_relevant_cols_rows(self, keep_movement_fromlink_level_):
        """
        Parameters
        ----------
        keep_movement_fromlink_level_
            fromlink levels that should be processed. For Tobin bridge, we are only
            interested in level 1 and np.nan (no level data)
        """
        # Filter to relevant simulation runs based on keep_runs_.
        # Filter to relevant columns.
        # Only interested in queue lengths, vehicle counts, and vehicle delay for this
        # study.
        self.node_eval_res_fil = (
            self.node_eval_res.loc[
                lambda df: (
                    (df.movementevaluation_simrun.isin(self.keep_runs))
                    & (
                        df.movement_fromlink_level.isin(keep_movement_fromlink_level_)
                    )  # 1 or empty cells are  for arterial roads.
                )
            ]
            .filter(items=self.keep_cols_cor_nm)
            .assign(
                node_no=lambda df: df.movement.str.extract(r"(\d*).*?").astype(int),
                from_link=lambda df: df.movement.str.extract(
                    r"(?:[^:]*)?(?::\W?)?([^@]*)?"
                ).transform(lambda x: x.str.strip()),
                to_link=lambda df: df.movement.str.extract(
                    r"(?:[^@]*)?(?:[^:]*)?(?::\W?)?([^@]*)?"
                ).transform(lambda x: x.str.strip()),
            )
        )

    def add_report_directions(self):
        """
        Get the direction values that would be used in the report. Different from what
        vissim output has. E.g. vissim direction N-S would be converted to SB.
        """
        # If no duplicates found in vissim then movement_direction_unique is the same as
        # movement_direction.
        if self.node_eval_deduplicate is None:
            self.node_eval_res_fil_uniq_dir = self.node_eval_res_fil.assign(
                movement_direction_unique=lambda df: df.movement_direction
            ).drop(columns="movement_direction")
        else:
            # If duplicates found then use the node_eval_deduplicate data to get unique
            # directions.
            self.node_eval_res_fil_uniq_dir = self.deduplicate_repeated_dirs()
        # Merge to node_eval_mapper after deduplicating directions to get report
        # directions.
        self.node_eval_res_fil_uniq_dir = (
            self.node_eval_res_fil_uniq_dir.merge(
                self.node_eval_mapper.assign(
                    movement_direction_unique=lambda df: df.movement_direction_unique.str.strip()
                ),
                on=["node_no", "movement_direction_unique"],
                how="left",
            )
            .loc[lambda df: df.movement_direction_unique != "Total"]
            .assign(main_dir=lambda df: df.direction_results.str.extract(r"(\S{2})"),)
        )

    def test_deduplicate_has_correct_values(self):
        """
        Test if the node_eval_deduplicate "node_no", "movement_direction", "from_link",
        "to_link" values matches what vissim outputs.
        """
        if self.node_eval_deduplicate is None:
            print("No duplicates.")
            return 1
        node_eval_deduplicate_test = self.node_eval_deduplicate.merge(
            self.node_eval_res_fil.filter(
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
        else:
            print(
                "De-duplication data has correct movement_direction, to_link, or/ and "
                "from_link values."
            )

    def deduplicate_repeated_dirs(self):
        """
        Handle duplicates by using the node_eval_deduplicate data to get unique
        directions.
        """
        node_eval_res_avg_uniq_dir = (
            self.node_eval_res_fil.merge(
                self.node_eval_deduplicate,
                on=["node_no", "movement_direction", "from_link", "to_link"],
                how="left",
            )
            .assign(
                movement_direction_unique=(
                    lambda df: df.movement_direction_unique.fillna(
                        df.movement_direction
                    )
                )
            )
            .drop(columns="movement_direction")
        )
        return node_eval_res_avg_uniq_dir

    def test_unique_dir_per_node(self):
        """
        Test if we have unique direction per node.
        """
        assert (
            self.node_eval_res_fil_uniq_dir.groupby(
                [
                    "movementevaluation_simrun",
                    "timeint",
                    "node_no",
                    "movement_direction_unique",
                ]
            )["movement_direction_unique"]
            .count()
            .max()
            == 1
        ), ("Fix issue with duplicate " "directions within same node.")
        print("No duplicate direction found within same node.")

    def get_veh_delay_by_intersection(self):
        """
        Get vehicle delay for all vehicles by intersection.
        """
        self.node_intersection_delay = (
            self.node_eval_res_fil_uniq_dir.filter(
                items=[
                    "movementevaluation_simrun",
                    "timeint",
                    "node_no",
                    "vehs_all",
                    "vehdelay_all",
                ]
            )
            .assign(
                tot_intersection_veh=lambda df: df.groupby(
                    ["movementevaluation_simrun", "timeint", "node_no"]
                ).vehs_all.transform(sum),
                veh_into_veh_delay=lambda df: df.vehs_all
                * df.vehdelay_all
                / df.tot_intersection_veh,
            )
            .groupby(["movementevaluation_simrun", "timeint", "node_no"])
            .agg(vehdelay_all=("veh_into_veh_delay", sum))
            .reset_index()
            .assign(direction_results="Intersection")
        )

    def get_veh_delay_by_approach(self):
        """
        Get vehicle delay for all vehicles by approach.
        """
        self.node_approach_delay = (
            self.node_eval_res_fil_uniq_dir.filter(
                items=[
                    "movementevaluation_simrun",
                    "timeint",
                    "node_no",
                    "main_dir",
                    "vehs_all",
                    "vehdelay_all",
                ]
            )
            .assign(
                tot_intersection_veh=lambda df: df.groupby(
                    ["movementevaluation_simrun", "timeint", "node_no", "main_dir"]
                ).vehs_all.transform(sum),
                veh_into_veh_delay=lambda df: df.vehs_all
                * df.vehdelay_all
                / df.tot_intersection_veh,
            )
            .groupby(["movementevaluation_simrun", "timeint", "node_no", "main_dir"])
            .agg(vehdelay_all=("veh_into_veh_delay", sum))
            .reset_index()
            .assign(direction_results=lambda df: df.main_dir)
        )

    def set_report_data(self, df_list):
        """
        Concat results by direction, approach, and intersection. Assign node type:
        signalized or twsc
        Parameters
        ----------
        df_list: list
            list of dataframes. Consisting of results by direction, approach, and
            intersection.
        """
        self.report_data = pd.concat(df_list)
        self.report_data.node_type = self.report_data.node_no.replace(
            self.node_no_node_type
        )

    def set_los(self):
        """
        Set LOS based on intersection type.
        """
        self.report_data = self.report_data.assign(
            los=lambda df: np.select(
                [
                    df.node_type.str.lower() == "signalized",
                    df.node_type.str.lower() == "twsc",
                ],
                [
                    df.vehdelay_all.apply(los_calc_signal),
                    df.vehdelay_all.apply(los_calc_twsc),
                ],
            )
        )
        # Todo: write this function

    def format_report_table(
        self,
        order_direction_results_,
        order_timeint_,
        results_cols_,
        order_timeint_label_,
    ):
        """

        Parameters
        ----------
        order_direction_results_: list
            Order for the directions.
        order_timeint_: list
            Order for time interval.
        results_cols_: list
            Order for the result columns.
        order_timeint_label_: dict
            label for order_timeint_
        """
        # Missing directions would not be included in the report. These are for Freeway
        # , bikepath or crosswalk.
        report_data_fil = self.report_data.loc[lambda df: ~df.direction_results.isna()]
        report_data_fil_pivot = (
            report_data_fil.assign(
                timeint=lambda df: pd.Categorical(df.timeint, order_timeint_),
                timeint_label=lambda df: df.timeint.replace(order_timeint_label_),
                direction_results=lambda df: pd.Categorical(
                    df.direction_results.str.strip(), order_direction_results_
                ),
            )
            .sort_values(["timeint_label", "node_no", "direction_results"])
            .reset_index(drop=True)
            .filter(
                items=[
                    "movementevaluation_simrun",
                    "node_no",
                    "main_dir",
                    "direction_results",
                    "timeint_label",
                    "from_link",
                    "to_link",
                ]
                + results_cols_
            )
            .set_index(
                [
                    "movementevaluation_simrun",
                    "node_no",
                    "main_dir",
                    "direction_results",
                    "from_link",
                    "to_link",
                    "timeint_label",
                ]
            )
            .unstack()
            .swaplevel(axis=1)
            .sort_index()
        )
        mux = pd.MultiIndex.from_product(
            [order_timeint_label_.values(), results_cols_], names=["timeint_label", ""],
        )
        self.report_data_fil_pivot = report_data_fil_pivot.reindex(mux, axis=1)

    def save_output_file(self):
        """
        Save the node evaluation output.
        """
        self.report_data_fil_pivot.to_excel(self.path_to_output_node_data)


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
        path_to_raw_data, "Tobin Bridge Base Model_Node Results.att"
    )
    path_to_output_node_data = os.path.join(
        path_to_interim_data, "process_node_eval.xlsx"
    )

    # 2. Set columns to keep, direction order, time interval order, columns to include
    # in results.
    # ************************************************************************************
    # Columns required for result processing.
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
    order_timeint_labels_am = {
        "2700-6300": "6:00-7:00 am",
        "6300-9900": "7:00-8:00 am",
        "9900-13500": "8:00-9:00 am",
        "13500-14400": "9:00-9:15 am",
    }
    # Sort order for the report results column.
    results_cols = ["qlen", "qlenmax", "vehs_all", "vehdelay_all", "los"]

    node_eval_am = NodeEval(
        path_to_mapper_node_eval_=path_to_mapper_node_eval,
        path_to_node_eval_res_=path_to_node_eval_res_am,
        path_to_output_node_data_=path_to_output_node_data,
        remove_duplicate_dir=True,
    )
    # filter rows and columns of the raw vissim node evaluation data.
    # Assign unique directions to all movements.
    node_eval_am.clean_node_eval(
        keep_cols_=keep_cols,
        keep_runs_=["AVG"],
        keep_movement_fromlink_level_=[1, np.nan],
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
