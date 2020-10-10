import inflection
import pandas as pd
import numpy as np
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root
import os


def los_calc(delay):
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
    def __init__(
        self,
        path_to_prj_,
        path_to_raw_data_,
        path_to_interim_data_,
        path_to_mapper_node_eval_,
        path_to_node_eval_res_,
        path_to_output_node_data_,
        remove_duplicate_dir=False,
    ):
        self.path_to_prj = path_to_prj_
        self.path_to_raw_data = path_to_raw_data_
        self.path_to_interim_data = path_to_interim_data_
        self.path_to_mapper_node_eval = path_to_mapper_node_eval_
        self.path_to_node_eval_res = path_to_node_eval_res_
        self.path_to_output_node_data = path_to_output_node_data_
        # Mapper files for converting Vissim directions into traffic operation directions.
        self.node_eval_mapper = pd.read_excel(
            path_to_mapper_node_eval_, sheet_name="vissim_report_convertion"
        )
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
        keep_cols_cor_nm = remove_special_char_vissim_col(keep_cols_)
        self.keep_cols_cor_nm = keep_cols_cor_nm
        if type(keep_runs_) == str:
            self.keep_runs = [keep_runs_]
        else:
            self.keep_runs = keep_runs_
        self.filter_to_relevant_cols_rows(keep_movement_fromlink_level_)
        self.add_report_directions()

    def filter_to_relevant_cols_rows(self, keep_movement_fromlink_level_):
        # Only interested in queue lengths, vehicle counts, and vehicle delay for this
        # study. Use the average of all runs for results.
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
        if self.node_eval_deduplicate is None:
            self.node_eval_res_fil_uniq_dir = self.node_eval_res_fil.assign(
                movement_direction_unique=lambda df: df.movement_direction
            ).drop(columns="movement_direction")
        else:
            self.node_eval_res_fil_uniq_dir = self.deduplicate_repeated_dirs()

        self.node_eval_res_fil_uniq_dir = (
            self.node_eval_res_fil_uniq_dir.merge(
                self.node_eval_mapper.assign(
                    movement_direction_unique=lambda df:
                    df.movement_direction_unique.str.strip()
                ),
                on=["node_no", "movement_direction_unique"],
                how="left",
            )
            .loc[lambda df: df.movement_direction_unique != "Total"]
            .assign(main_dir=lambda df: df.direction_results.str.extract(r"(\S{2})"),)
        )

    def test_deduplicate_has_correct_values(self):
        if self.node_eval_deduplicate is None:
            print("No de-duplication data found or used.")
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
        self.report_data = pd.concat(df_list)
        self.report_data.node_type = self.report_data.node_no.replace(
            self.node_no_node_type
        )

    def set_los(self):
        self.report_data = self.report_data.assign(
            los=lambda df: np.select(
                [
                    df.node_type.str.lower() == "signalized",
                    df.node_type.str.lower() == "twsc",
                ],
                [df.vehdelay_all.apply(los_calc), df.vehdelay_all.apply(los_calc_twsc)],
            )
        )
        # Todo: write this function

    def format_report_table(
        self, order_direction_results_, order_timeint_, results_cols_
    ):
        report_data_fil = self.report_data.loc[lambda df: ~df.direction_results.isna()]
        report_data_fil_pivot = (
            report_data_fil.assign(
                timeint=lambda df: pd.Categorical(df.timeint, order_timeint_),
                direction_results=lambda df: pd.Categorical(
                    df.direction_results.str.strip(), order_direction_results_
                ),
            )
            .sort_values(["timeint", "node_no", "direction_results"])
            .reset_index(drop=True)
            .filter(
                items=[
                    "movementevaluation_simrun",
                    "node_no",
                    "main_dir",
                    "direction_results",
                    "timeint",
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
                    "timeint",
                ]
            )
            .unstack()
            .swaplevel(axis=1)
            .sort_index()
        )
        mux = pd.MultiIndex.from_product(
            [order_timeint_, results_cols_,], names=["timeint", ""],
        )
        self.report_data_fil_pivot = report_data_fil_pivot.reindex(mux, axis=1)

    def save_output_file(self):
        self.report_data_fil_pivot.to_excel(self.path_to_output_node_data)


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

    node_eval_am = NodeEval(
        path_to_prj_=path_to_prj,
        path_to_raw_data_=path_to_raw_data,
        path_to_interim_data_=path_to_interim_data,
        path_to_mapper_node_eval_=path_to_mapper_node_eval,
        path_to_node_eval_res_=path_to_node_eval_res_am,
        path_to_output_node_data_=path_to_output_node_data,
        remove_duplicate_dir=True,
    )

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
    results_cols = ["qlen", "qlenmax", "vehs_all", "vehdelay_all", "los"]

    node_eval_am.format_report_table(
        order_direction_results_=order_direction_results,
        order_timeint_=order_timeint,
        results_cols_=results_cols,
    )

    node_eval_am.save_output_file()
