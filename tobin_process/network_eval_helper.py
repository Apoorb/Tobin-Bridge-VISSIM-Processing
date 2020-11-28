import inflection
import pandas as pd
import numpy as np
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root
import os


def network_eval_processing(
    paths_network_eval_vissim_, order_timeint_, order_timeint_labels_am_
):
    timeint_dict = {x: y for (x, y) in zip(order_timeint_, order_timeint_labels_am_)}

    network_eval = pd.read_csv(
        paths_network_eval_vissim_, comment="*", sep=";", skiprows=1
    )
    network_eval.columns = remove_special_char_vissim_col(network_eval.columns)

    network_eval_fil = (
        network_eval.loc[
            lambda df: df.vehiclenetworkperformancemeasurementevaluation_simrun.isin(
                [1]
            )
        ]
        .filter(
            items=[
                "vehiclenetworkperformancemeasurementevaluation_simrun",
                "timeint",
                "delayavg_all",
                "vehact_all",
                "veharr_all",
                "delaylatent",
                "demandlatent",
            ]
        )
        .assign(timeint=lambda df: df.timeint.map(timeint_dict))
    )
    return network_eval_fil


def save_output(network_eval_fil_, path_to_output_network_eval_):
    network_eval_fil_.to_excel(path_to_output_network_eval_)


if __name__ == "__main__":
    # 1. Set the paths for input files and output files.
    # ************************************************************************************
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")
    paths_network_eval_vissim = os.path.join(
        path_to_raw_data,
        "Tobin Bridge Base Model - AM Peak Hour V2_Vehicle Network Performance Evaluation Results.att",
    )
    path_to_output_network_eval = os.path.join(
        path_to_interim_data, "process_network_eval.xlsx"
    )
    # 2. Set time interval, time interval labels, report vehicle classes mapping to
    # vehicle types, occupancy by vissim vehicle type, results column to retain,
    # travel time segments to keep.
    # in results.
    # ************************************************************************************
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

    network_eval_fil = network_eval_processing(
        paths_network_eval_vissim_=paths_network_eval_vissim,
        order_timeint_=order_timeint,
        order_timeint_labels_am_=order_timeint_labels_am,
    )

    save_output(
        network_eval_fil_=network_eval_fil,
        path_to_output_network_eval_=path_to_output_network_eval,
    )
