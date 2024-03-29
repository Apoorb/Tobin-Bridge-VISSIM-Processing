import os
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root
import tobin_process.link_seg_helper as link_helper


if __name__ == "__main__":
    # 1. Set the paths for input files and output files.
    # ************************************************************************************
    path_to_prj = get_project_root()
    path_to_raw_data = os.path.join(path_to_prj, "data", "raw")
    path_to_interim_data = os.path.join(path_to_prj, "data", "interim")
    path_to_mappers_data = os.path.join(path_to_prj, "data", "mappers")
    path_to_mapper_link_seg = os.path.join(
        path_to_mappers_data, "link_seg_mapping.xlsx"
    )
    path_link_seg_vissim = os.path.join(
        path_to_raw_data,
        "Tobin Bridge Base Model - AM Peak Period V3_Link Segment Results.att",
    )
    path_to_output_fig = os.path.join(path_to_interim_data, "figures")
    if not os.path.exists(path_to_output_fig):
        os.mkdir(path_to_output_fig)
    path_to_output_link_seg_fig = os.path.join(
        path_to_output_fig, "am_figures_link_seg"
    )
    if not os.path.exists(path_to_output_link_seg_fig):
        os.mkdir(path_to_output_link_seg_fig)
    # 2. Set columns to keep, direction order, time interval order, columns to include
    # in results.
    # ************************************************************************************
    # Columns required for result processing.
    keep_cols = [
        "$LINKEVALSEGMENTEVALUATION:SIMRUN",  # would need for all projects
        "TIMEINT",  # would need for all projects
        "LINKEVALSEGMENT",  # would need for all projects
        r"LINKEVALSEGMENT\LINK\NUMLANES",  # would need for all projects
        r"DENSITY(1020)",  # would need for all projects
        r"SPEED(1020)",  # would need for all projects
        r"VOLUME(1020)",  # would need for all projects
    ]
    keep_cols = remove_special_char_vissim_col(keep_cols)
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
    # Vissim runs to output result for: 1,2, ... or "AVG".
    keep_runs = ["AVG"]

    link_seg_am = link_helper.LinkSegEval(
        path_to_mapper_link_seg_=path_to_mapper_link_seg,
        path_link_seg_vissim_=path_link_seg_vissim,
        path_to_output_link_seg_fig_=path_to_output_link_seg_fig,
    )
    link_seg_am.read_link_seg()

    link_seg_am.clean_filter_link_eval(
        keep_runs_=keep_runs,
        keep_cols_=keep_cols,
        order_timeint_=order_timeint,
        order_timeint_labels_=order_timeint_labels_am,
    )
    # Test if the analyst has set the link evaluation length to correct value in vissim.
    link_seg_am.test_seg_eval_len(eval_len=1000)

    link_seg_am.merge_link_mapper()

    link_seg_am.plot_heatmaps(
        plot_var="speed_1020",
        index_var="display_name",
        color_lab="Speed (mph)",
        zmin=0,
        zmax=60,
        yaxis_ticksuffix_="",
        xaxis_ticksuffix_="",
        margin_=dict(l=1200, pad=10),
        height_=1600,
        width_=1800,
        title_suffix="debug",
        colorscale_="viridis",
    )

    link_seg_am.plot_heatmaps(
        plot_var="speed_1020",
        index_var="cum_offset",
        color_lab="Speed (mph)",
        zmin=0,
        zmax=60,
        yaxis_ticksuffix_=" mi",
        xaxis_ticksuffix_=" am",
        margin_=dict(pad=10),
        height_=800,
        width_=1000,
        title_suffix="miles",
        colorscale_="viridis",
    )

    link_seg_am.link_seg_vissim_fil_ord = link_seg_am.link_seg_vissim_fil_ord.assign(
        density_1020_by_ln=lambda df: df.density_1020 / df.linkevalsegment_link_numlanes
    )
    link_seg_am.plot_heatmaps(
        plot_var="density_1020_by_ln",
        index_var="display_name",
        color_lab="Density<br>(veh/mi/ln)",
        zmin=0,
        zmax=120,
        colorscale_="viridis_r",
        yaxis_ticksuffix_="",
        xaxis_ticksuffix_="",
        margin_=dict(l=1200, pad=10),
        height_=1600,
        width_=1800,
        title_suffix="debug",
    )

    link_seg_am.plot_heatmaps(
        plot_var="density_1020_by_ln",
        index_var="cum_offset",
        color_lab="Density<br>(veh/mi/ln)",
        colorscale_="viridis_r",
        zmin=0,
        zmax=120,
        yaxis_ticksuffix_=" mi",
        xaxis_ticksuffix_=" am",
        margin_=dict(pad=10),
        title_suffix="miles",
        height_=800,
        width_=1000,
    )
