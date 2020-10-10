import pandas as pd
import numpy as np
import os
import glob
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root
import seaborn as sns
import matplotlib.pyplot as plt


if __name__ == "__main__":
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")
    path_to_mappers_data = os.path.join(path_to_prj, "data", "mappers")
    path_to_mapper_link_seg = os.path.join(path_to_mappers_data, "link_seg_mapping.xlsx")
    path_link_seg_vissim = os.path.join(
        path_to_raw_data,
        "Tobin Bridge Base Model - AM Peak Hour_Link Segment Results.att"
    )
    path_to_output_fig = os.path.join(path_to_interim_data, "figures")
    if not os.path.exists(path_to_output_fig):
        os.mkdir(path_to_output_fig)
    path_to_output_link_seg_fig = os.path.join(path_to_output_fig, "am_figures_link_seg")
    if not os.path.exists(path_to_output_link_seg_fig):
        os.mkdir(path_to_output_link_seg_fig)


    link_seg_mapper = pd.read_excel(path_to_mapper_link_seg)


    keep_cols = [
        "$LINKEVALSEGMENTEVALUATION:SIMRUN",  # would need for all projects
        "TIMEINT",  # would need for all projects
        "LINKEVALSEGMENT",  # would need for all projects
        r"LINKEVALSEGMENT\LINK\NUMLANES",  # would need for all projects
        r"DENSITY(1020)",  # would need for all projects
        r"SPEED(1020)",  # would need for all projects
    ]

    keep_cols_ = remove_special_char_vissim_col(keep_cols)
    keep_runs_ = [1]
    keep_link = []
    # * is comment line. $ also means comment, but in pandas we can only use one
    # char for denoting comment, so using skiprow=1 to skip the 1st row, which has
    # a $ sign. 2nd and last $ sign is used with column name. Will address it below.
    link_seg_vissim = pd.read_csv(
        path_link_seg_vissim, comment="*", sep=";", skiprows=1
    )
    # Remove special charaters from the Vissim names.
    link_seg_vissim.columns = remove_special_char_vissim_col(link_seg_vissim.columns)
    link_seg_vissim[["link", "st_pt", "end_pt"]] = link_seg_vissim.linkevalsegment.str.split("-", expand=True)
    link_seg_vissim[["link", "st_pt", "end_pt"]] = link_seg_vissim[["link", "st_pt", "end_pt"]].values.astype(int)
    link_seg_vissim_fil = (
        link_seg_vissim.loc[
            lambda df: (
                    (df.linkevalsegmentevaluation_simrun.isin(keep_runs_))
                    & (
                        df.link.isin(list(link_seg_mapper.link.values))
                    )  # 1 or empty cells are  for arterial roads.
            )
        ]
            .filter(items=keep_cols_+["link", "st_pt", "end_pt"])
    )

    a = np.arange(0, 1000 * (np.ceil(link_seg_vissim_fil.end_pt.max() / 1000) + 1), 1000)
    link_seg_vissim_fil_ord = (
        link_seg_vissim_fil
            .merge(
                link_seg_mapper,
                on="link",
                how="right"
        )
        .sort_values(
            ["linkevalsegmentevaluation_simrun", "timeint", "direction", "order", "st_pt"]
        )
        .assign(
            subseg_len=lambda df: df.end_pt - df.st_pt,
            bin_1000_ft=lambda df: pd.cut(df.end_pt, a)
        )
    )

    link_seg_vissim_fil_ord_grp = (
        link_seg_vissim_fil_ord
        .groupby(["linkevalsegmentevaluation_simrun", "timeint", "direction", "order", "bin_1000_ft"])
        .agg(
            link=("link", "first"),
            subseg_len=("subseg_len", "sum"),
             avg_speed=("speed_1020", "mean"),
             avg_density=("density_1020", "mean"))
        .dropna(axis=0)
    )