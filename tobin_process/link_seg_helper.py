import pandas as pd
import os
from tobin_process.utils import remove_special_char_vissim_col
from tobin_process.utils import get_project_root
import plotly.graph_objects as go
import plotly.io as pio

pio.renderers.default = "browser"


class LinkSegEval:
    def __init__(
        self,
        path_to_mapper_link_seg_,
        path_link_seg_vissim_,
        path_to_output_link_seg_fig_,
    ):
        """
        Parameters
        ----------
        path_to_mapper_link_seg_: str
            Path to the link segment mapper file.
        path_link_seg_vissim_: str
            Path to the vissim link evaluation file.
        path_to_output_link_seg_fig_: str
            Path for storing the output figures.
        """
        self.path_to_mapper_link_seg = path_to_mapper_link_seg_
        self.path_link_seg_vissim = path_link_seg_vissim_
        self.path_to_output_link_seg_fig = path_to_output_link_seg_fig_
        # Mapper file to get link names.
        self.link_seg_mapper = pd.read_excel(self.path_to_mapper_link_seg)
        link_seg_vissim = self.read_link_seg()
        self.link_seg_vissim_fil = pd.DataFrame()
        self.link_seg_vissim_fil_ord = pd.DataFrame()

    def read_link_seg(self):
        """
        Read vissim link segment evaluation data.
        """
        # * is comment line. $ also means comment, but in pandas we can only use one
        # char for denoting comment, so using skiprow=1 to skip the 1st row, which has
        # a $ sign. 2nd and last $ sign is used with column name. Will address it below.
        self.link_seg_vissim = pd.read_csv(
            self.path_link_seg_vissim, comment="*", sep=";", skiprows=1
        )
        # Remove special charaters from the Vissim names.
        self.link_seg_vissim.columns = remove_special_char_vissim_col(
            self.link_seg_vissim.columns
        )
        self.link_seg_vissim[
            ["link", "st_pt", "end_pt"]
        ] = self.link_seg_vissim.linkevalsegment.str.split("-", expand=True)
        self.link_seg_vissim[["link", "st_pt", "end_pt"]] = self.link_seg_vissim[
            ["link", "st_pt", "end_pt"]
        ].values.astype(int)

    def test_seg_eval_len(self, eval_len=1000):
        """
        Test if the analyst has set the link evaluation length to correct value in vissim.
        """
        assert all(
            self.link_seg_vissim_fil.st_pt % eval_len == 0
        ), f"Change link evaluation segment length to {eval_len} ft. in Vissim."

    def clean_filter_link_eval(
        self, keep_runs_, keep_cols_, order_timeint_, order_timeint_labels_
    ):
        """
        Filter rows and columns of the raw vissim node evaluation data.
        Assign unique directions to all movements.
        Parameters
        ----------
        keep_cols_: list
            Columns to keep.
        keep_runs_: list
            Runs to process. Generaly would only be interested in average results.
        order_timeint: Order of timeint.
        order_timeint_labels_: Labels for the timeint.
        """
        if type(keep_runs_) == str:
            keep_runs_ = [keep_runs_]
        else:
            keep_runs_ = keep_runs_

        self.link_seg_vissim_fil = (
            self.link_seg_vissim.loc[
                lambda df: (
                    (df.linkevalsegmentevaluation_simrun.isin(keep_runs_))
                    & (
                        df.link.isin(list(self.link_seg_mapper.link.values))
                    )  # 1 or empty cells are  for arterial roads.
                )
            ]
            .assign(
                timeint=lambda df: pd.Categorical(df.timeint, categories=order_timeint_)
            )
            .filter(items=keep_cols_ + ["link", "st_pt", "end_pt"])
        )

        self.link_seg_vissim_fil.timeint = self.link_seg_vissim_fil.timeint.cat.rename_categories(
            {i: j for (i, j) in zip(order_timeint_, order_timeint_labels_)}
        )

    def merge_link_mapper(self):
        """
        Merge link mapper.
        """
        self.link_seg_vissim_fil_ord = self.link_seg_vissim_fil.merge(
            self.link_seg_mapper, on="link", how="right"
        ).sort_values(
            [
                "linkevalsegmentevaluation_simrun",
                "timeint",
                "direction",
                "order",
                "st_pt",
            ]
        )
        self.link_seg_vissim_fil_ord = self.link_seg_vissim_fil_ord.assign(
            st_end_diff=lambda df: df.end_pt - df.st_pt,
            cum_offset=lambda df: df.groupby(
                ["linkevalsegmentevaluation_simrun", "timeint", "direction"]
            ).st_end_diff.cumsum()
            / 5280,
        )

    def plot_heatmaps(
        self,
        plot_var,
        index_var,
        color_lab,
        zmin,
        zmax,
        yaxis_ticksuffix_,
        xaxis_ticksuffix_,
        title_suffix,
        margin_,
        height_,
        width_,
        zmid=None,
        colorscale_="viridis",
    ):
        """
        Parameters
        ----------
        plot_var: str
            Variable for defining color scale.
        color_lab: str
            Label for the plot_var.
        """
        plot_grps = self.link_seg_vissim_fil_ord.groupby(
            ["linkevalsegmentevaluation_simrun", "direction"]
        )

        for name, group in plot_grps:
            plot_grp_reshaped = (
                pd.pivot_table(
                    (
                        group.assign(
                            display_name=lambda df: df[
                                ["display_name", "st_pt", "end_pt"]
                            ].apply(
                                lambda x: str(x[0])
                                + " "
                                + str(x[1])
                                + "-"
                                + str(x[2])
                                + " ft",
                                axis=1,
                            )
                        ).assign(
                            display_name=lambda df: pd.Categorical(
                                df.display_name, df.display_name.unique()
                            )
                        )
                    ),
                    values=plot_var,
                    columns="timeint",
                    index=index_var,
                )
                .sort_index(axis=1, ascending=True)
                .sort_index(axis=0, ascending=True)
            )

            fig = go.Figure(
                data=go.Heatmap(
                    x=plot_grp_reshaped.columns,
                    y=plot_grp_reshaped.index,
                    z=plot_grp_reshaped.values,
                    type="heatmap",
                    colorscale=colorscale_,
                    colorbar_thickness=15,
                    colorbar_title=color_lab,
                    zmin=zmin,
                    zmax=zmax,
                    zmid=zmid
                )
            )
            if name[1] == "SB":
                fig.update_layout(yaxis_autorange="reversed")
            fig.update_layout(
                margin=margin_,
                autosize=True,
                font=dict(size=22),
                plot_bgcolor="rgba(0,0,0,0)",
                yaxis_ticksuffix=yaxis_ticksuffix_,
                xaxis_ticksuffix=xaxis_ticksuffix_,
                xaxis_tickangle=-45
            )
            path_to_output_tt_fig_filenm_html = os.path.join(
                self.path_to_output_link_seg_fig,
                "_".join([str(name[0]), name[1], plot_var, title_suffix, ".html"]),
            )
            fig.write_html(path_to_output_tt_fig_filenm_html)
            fig.update_layout(
                height=height_,
                width=width_,
                margin=margin_,
                autosize=False,
                font=dict(size=22),
                plot_bgcolor="rgba(0,0,0,0)",
                yaxis_ticksuffix=yaxis_ticksuffix_,
                xaxis_ticksuffix=xaxis_ticksuffix_,
            )

            path_to_output_tt_fig_filenm = os.path.join(
                self.path_to_output_link_seg_fig,
                "_".join([str(name[0]), name[1], plot_var, title_suffix, ".jpg"]),
            )
            fig.write_image(path_to_output_tt_fig_filenm)


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
    # Vissim runs to output result for.
    keep_runs = ["AVG"]

    link_seg_am = LinkSegEval(
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
    link_seg_am.test_seg_eval_len()
    link_seg_am.merge_link_mapper()

    link_seg_am.plot_heatmaps(
        plot_var="speed_1020",
        index_var="display_name",
        color_lab="Speed (mph)",
        zmin=0,
        zmax=50,
        yaxis_ticksuffix_="",
        xaxis_ticksuffix_="",
        margin_=dict(l=1200, pad=10),
        height_=1600,
        width_=1800,
        title_suffix="debug",
        colorscale_="viridis"
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
        zmid=15,
        colorscale_="RdYlGn"
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
