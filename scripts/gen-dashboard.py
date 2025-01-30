import sys
from typing import Union, List, Literal

from dashboard_builder import (
    Layout,
    timeseries_panel,
    target,
    template,
    Expr,
    Stat,
    expr_sum_rate,
    expr_sum_increase,
    expr_aggr_func,
    expr_avg,
    heatmap_panel,
    yaxis,
    expr_operator,
    expr_max,
    DATASOURCE,
)
from grafanalib import formatunits as UNITS, _gen
from grafanalib.core import (
    Dashboard,
    Templating,
    Template,
    Annotations,
    RowPanel,
    Panel,
    HeatmapColor,
    Tooltip,
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    Target,
)


# todo: do something with this metrics
# tycho_core_last_mc_block_applied
# tycho_core_last_sc_block_applied
# tycho_core_last_sc_block_seqno
# tycho_core_last_sc_block_utime


def heatmap_color_warm() -> HeatmapColor:
    return HeatmapColor()


def generate_legend_format(labels: List[str]) -> str:
    """
    Generate a legend format string based on the provided labels.

    Args:
    labels (List[str]): A list of label strings.

    Returns:
    str: A legend format string including instance and provided labels with keys.
    """
    legend_format = "{{instance}}"
    for label in labels:
        key = label.split("=")[0]  # Extract the key part before '='
        legend_format += f" {key}:{{{{{key}}}}}"
    return legend_format


def create_gauge_panel(
        expr: Union[str, List[Union[str, Expr]], Expr],
        title: str,
        unit_format=UNITS.NUMBER_FORMAT,
        labels=[],
        legend_format: str | None = None,
) -> Panel:
    if isinstance(expr, str):
        expr = [Expr(metric=expr, label_selectors=labels)]
    elif isinstance(expr, list):
        expr = [
            Expr(metric=e, label_selectors=labels) if isinstance(e, str) else e
            for e in expr
        ]
    elif isinstance(expr, Expr):
        expr = [expr]
    else:
        raise TypeError(
            "expr must be a string, a list of strings, or a list of Expr objects."
        )

    if legend_format is None:
        legend_format = generate_legend_format(labels)

    targets = [target(e, legend_format=legend_format) for e in expr]

    return timeseries_panel(
        title=title,
        targets=targets,
        unit=unit_format,
    )


def create_counter_panel(
        expr: Union[str | Expr, List[Union[str, Expr]]],
        title: str,
        unit_format: str = UNITS.NUMBER_FORMAT,
        labels_selectors: List[str] = [],
        legend_format: str | None = None,
        by_labels: list[str] = ["instance"],
) -> Panel:
    """
    Create a counter panel for visualization.

    Args:
        expr (Union[str, List[Union[str, Expr]]]): Expression or list of expressions to visualize.
        title (str): Title of the panel.
        unit_format (str, optional): Format for the unit display. Defaults to UNITS.NUMBER_FORMAT.
        labels_selectors (List[str], optional): List of label selectors. Defaults to an empty list.
        legend_format (str | None, optional): Format for the legend. If None, it's generated automatically. Defaults to None.
        by_labels (list[str], optional): Labels to group by. Defaults to ["instance"].

    Returns:
        Panel: A timeseries panel object.

    Raises:
        ValueError: If the list elements in expr are not all strings or all Expr objects.
        TypeError: If expr is not a string, a list of strings, or a list of Expr objects.
    """
    if legend_format is None:
        legend_format = generate_legend_format(labels_selectors)

    if isinstance(expr, str):
        targets = [
            target(
                expr_sum_rate(
                    expr, label_selectors=labels_selectors, by_labels=by_labels
                ),
                legend_format=legend_format,
            )
        ]
    elif isinstance(expr, list):
        if all(isinstance(e, str) for e in expr):
            targets = [
                target(
                    expr_sum_rate(e, label_selectors=labels_selectors),
                    legend_format=legend_format,
                )
                for e in expr
            ]
        elif all(isinstance(e, Expr) for e in expr):
            targets = [target(e, legend_format=legend_format) for e in expr]
        else:
            raise ValueError("List elements must be all strings or all Expr objects.")
    elif isinstance(expr, Expr):
        targets = [target(expr, legend_format=legend_format)]
    else:
        raise TypeError(
            "expr must be a string, a list of strings, or a list of Expr objects."
        )

    return timeseries_panel(
        title=title,
        targets=targets,
        unit=unit_format,
    )


def create_percent_panel(
        metric1: str,
        metric2: str,
        title: str,
        group_by_labels: List[str] = ["instance"],
        label_selectors: List[str] = [],
        unit_format: str = UNITS.PERCENT_FORMAT,
) -> Panel:
    """
    create a panel showing the percentage of metric1 to metric2, grouped by specified labels.

    Args:
        metric1 (str): The first metric (numerator).
        metric2 (str): The second metric (denominator).
        title (str): Title of the panel.
        group_by_labels (List[str]): Labels to group by and match on.
        label_selectors (List[str]): Additional label selectors for both metrics.
        unit_format (str, optional): Format for the unit display. defaults to UNITS.PERCENT_FORMAT.

    Returns:
        Panel: A timeseries panel object showing the percentage.
    """
    expr1 = expr_sum_rate(
        metric1, label_selectors=label_selectors, by_labels=group_by_labels
    )
    expr2 = expr_sum_rate(
        metric2, label_selectors=label_selectors, by_labels=group_by_labels
    )

    percent_expr = expr_operator(expr_operator(expr1, "/", expr2), "*", "100")

    legend_format = "{{" + "}} - {{".join(group_by_labels) + "}}"

    percent_target = target(percent_expr, legend_format=legend_format)

    return timeseries_panel(title=title, targets=[percent_target], unit=unit_format)


def create_heatmap_panel(
        metric_name,
        title,
        unit_format=yaxis(UNITS.SECONDS),
        labels=[],
) -> Panel:
    return heatmap_panel(
        title,
        f"{metric_name}_bucket",
        yaxis=unit_format,
        color=heatmap_color_warm(),
        tooltip=Tooltip(),
        label_selectors=labels,
        rate_interval="10s",  # todo: update this if scrape interval changes
    )


# Type alias for accepted quantiles
ACCEPTED_QUANTILES = {"0", "0.5", "0.9", "0.95", "0.99", "0.999", "1"}
AcceptedQuantile = Literal["0", "0.5", "0.9", "0.95", "0.99", "0.999", "1"]


def create_heatmap_quantile_panel(
        metric_name: str,
        title: str,
        unit_format: str = UNITS.NUMBER_FORMAT,
        quantile: AcceptedQuantile = "0.95",
) -> Panel:
    """
    Create a heatmap quantile panel for the given metric.

    Args:
        metric_name (str): Name of the metric to visualize.
        title (str): Title of the panel.
        unit_format (str, optional): Unit format for the panel. Defaults to UNITS.NUMBER_FORMAT.
        quantile (AcceptedQuantile, optional): Quantile to use (as an integer 0-100). Defaults to 95.

    Returns:
        Panel: A configured grafanalib Panel object.

    Raises:
        ValueError: If the quantile is not one of the accepted values.
    """

    if quantile not in ACCEPTED_QUANTILES:
        raise ValueError(f"Quantile must be one of {ACCEPTED_QUANTILES}")

    legend_format = f"{{{{instance}}}} p{quantile}"
    quantile_expr = f'quantile="{quantile}"'

    return timeseries_panel(
        title=title,
        targets=[
            target(
                expr=Expr(metric_name, label_selectors=[quantile_expr]),
                legend_format=legend_format,
            )
        ],
        unit=unit_format,
    )


def create_row(
        name: str, metrics, repeat: str | None = None, collapsed=True
) -> RowPanel:
    layout = Layout(name, repeat=repeat, collapsed=collapsed)
    for i in range(0, len(metrics), 2):
        chunk = metrics[i: i + 2]
        layout.row(chunk)
    return layout.row_panel


def archives_uploader() -> RowPanel:
    first_row = [
        Stat(
            targets=[
                Target(
                    expr=f"""{expr_max(
                        'tycho_core_last_mc_block_seqno',
                        by_labels=[]
                    )}""",
                    legendFormat="Last MC Block",
                    instant=True,
                    datasource=DATASOURCE,
                ),
                Target(
                    expr=f"""{expr_max(
                        'tycho_uploader_last_uploaded_archive_seqno',
                        by_labels=[]
                    )}""",
                    legendFormat="Last Archive Id",
                    instant=True,
                    datasource=DATASOURCE,
                ),
            ],
            graphMode="area",
            textMode="value_and_name",
            reduceCalc="lastNotNull",
            format=UNITS.NONE_FORMAT,
        ),
        timeseries_panel(
            targets=[
                target(
                    expr_avg(
                        "tycho_uploader_archive_size",
                        label_selectors=['quantile="0.5"'],
                        by_labels=[],
                    ),
                    legend_format="P50",
                ),
                target(
                    expr_avg(
                        "tycho_uploader_archive_size",
                        label_selectors=['quantile="0.999"'],
                        by_labels=[],
                    ),
                    legend_format="P99",
                ),
            ],
            title="Archive Size",
            unit=UNITS.BYTES,
            legend_display_mode="hidden",
        ),
        create_heatmap_panel(
            "tycho_uploader_upload_archive_time", "Time to upload archive"
        ),
    ]

    layout = Layout("Archives Uploader", repeat=None, collapsed=True)
    layout.row(first_row)
    return layout.row_panel


def core_block_strider() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_core_process_strider_step_time",
            "Time to process block strider step",
        ),
        create_heatmap_panel(
            "tycho_core_provider_cleanup_time",
            "Time to cleanup block providers",
        ),
        create_heatmap_panel(
            "tycho_core_download_mc_block_time", "Masterchain block downloading time"
        ),
        create_heatmap_panel(
            "tycho_core_prepare_mc_block_time", "Masterchain block preparing time"
        ),
        create_heatmap_panel(
            "tycho_core_process_mc_block_time", "Masterchain block processing time"
        ),
        create_heatmap_panel(
            "tycho_core_download_sc_block_time", "Shard block downloading time"
        ),
        create_heatmap_panel(
            "tycho_core_prepare_sc_block_time",
            "Shard block preparing time",
        ),
        create_heatmap_panel(
            "tycho_core_process_sc_block_time",
            "Shard block processing time",
        ),
        create_heatmap_panel(
            "tycho_core_download_sc_blocks_time",
            "Total time to download all shard blocks",
        ),
        create_heatmap_panel(
            "tycho_core_process_sc_blocks_time",
            "Total time to process all shard blocks",
        ),
        create_heatmap_panel(
            "tycho_core_state_applier_prepare_block_time",
            "Time to prepare block by ShardStateApplier",
        ),
        create_heatmap_panel(
            "tycho_core_state_applier_handle_block_time",
            "Time to handle block by ShardStateApplier",
        ),
        create_heatmap_panel(
            "tycho_core_archive_handler_prepare_block_time",
            "Time to prepare block by ArchiveHandler",
        ),
        create_heatmap_panel(
            "tycho_core_archive_handler_handle_block_time",
            "Time to handle block by ArchiveHandler",
        ),
        create_heatmap_panel(
            "tycho_core_subscriber_handle_state_time",
            "Total time to handle state by all subscribers",
        ),
        create_heatmap_panel(
            "tycho_core_subscriber_handle_archive_time",
            "Total time to handle archive by all subscribers",
        ),
        create_heatmap_panel(
            "tycho_core_apply_block_time",
            "Time to apply and save block state",
        ),
        create_heatmap_panel(
            "tycho_core_metrics_subscriber_handle_block_time",
            "Time to handle block by MetricsSubscriber",
        ),
        create_heatmap_panel(
            "tycho_core_check_block_proof_time", "Check block proof time"
        ),
    ]
    return create_row("block strider: Core Metrics", metrics)


def storage() -> RowPanel:
    metrics = [
        create_heatmap_panel(
            "tycho_storage_load_cell_time", "Time to load cell from storage"
        ),
        create_counter_panel(
            expr_sum_rate("tycho_storage_load_cell_time_count"),
            "Number of load_cell calls",
            UNITS.OPS_PER_SEC,
        ),
        create_heatmap_panel(
            "tycho_storage_get_cell_from_rocksdb_time", "Time to load cell from RocksDB"
        ),
        create_counter_panel(
            expr_sum_rate("tycho_storage_get_cell_from_rocksdb_time_count"),
            "Number of cache missed cell loads",
            UNITS.OPS_PER_SEC,
        ),
        timeseries_panel(
            title="Storage Cache Hit Rate",
            targets=[
                target(
                    expr=expr_operator(
                        expr_operator(
                            "1",
                            "-",
                            expr_operator(
                                expr_sum_rate(
                                    "tycho_storage_get_cell_from_rocksdb_time_count",
                                ),
                                "/",
                                expr_sum_rate(
                                    "tycho_storage_load_cell_time_count",
                                ),
                            ),
                        ),
                        "*",
                        "100",
                    ),
                    legend_format="Hit Rate",
                )
            ],
            unit=UNITS.PERCENT_FORMAT,
        ),
        create_counter_panel(
            "tycho_storage_raw_cells_cache_size",
            "Raw cells cache size",
            UNITS.BYTES_IEC,
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_store_block_data_size",
            "Block data size",
            UNITS.BYTES_IEC,
            "0.999",
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_cell_count",
            "Number of new cells from merkle update",
            quantile="0.999",
        ),
        create_heatmap_panel(
            "tycho_storage_state_update_time", "Time to write state update to rocksdb"
        ),
        create_heatmap_panel(
            "tycho_storage_state_store_time",
            "Time to store single root with rocksdb write etc",
        ),
        create_heatmap_panel(
            "tycho_storage_cell_in_mem_store_time", "Time to store cell without write"
        ),
        create_heatmap_panel(
            "tycho_storage_cell_in_mem_store_time", "Time to store cell without write"
        ),
        create_heatmap_panel(
            "tycho_storage_batch_write_time", "Time to write merge in write batch"
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_state_update_size_bytes",
            "State update size",
            UNITS.BYTES,
            "0.999",
        ),
        create_heatmap_quantile_panel(
            "tycho_storage_state_update_size_predicted_bytes",
            "Predicted state update size",
            UNITS.BYTES,
            "0.999",
        ),
        create_heatmap_panel(
            "tycho_storage_batch_write_time", "Time to write merge in write batch"
        ),
        create_heatmap_panel(
            "tycho_storage_state_store_time", "Time to store state with cell traversal"
        ),
        create_heatmap_panel("tycho_gc_states_time", "Time to garbage collect state"),
        timeseries_panel(
            targets=[
                target(
                    expr_operator(
                        Expr(
                            metric="tycho_core_last_mc_block_seqno",
                        ),
                        "- on(instance, job)",
                        Expr("tycho_gc_states_seqno"),
                    ),
                    legend_format="{{instance}}",
                )
            ],
            unit="Blocks",
            title="GC lag",
        ),
        create_heatmap_panel(
            "tycho_storage_move_into_archive_time", "Time to move into archive"
        ),
        create_heatmap_panel(
            "tycho_storage_commit_archive_time", "Time to commit archive"
        ),
        create_heatmap_panel(
            "tycho_storage_split_block_data_time", "Time to split block data"
        ),
        create_gauge_panel(
            "tycho_storage_cells_tree_cache_size", "Cells tree cache size"
        ),
        create_counter_panel(
            "tycho_compaction_keeps", "Number of not deleted cells during compaction"
        ),
        create_counter_panel(
            "tycho_compaction_removes", "Number of deleted cells during compaction"
        ),
        create_counter_panel(
            "tycho_storage_state_gc_count", "number of deleted states during gc"
        ),
        create_counter_panel(
            "tycho_storage_state_gc_cells_count", "number of deleted cells during gc"
        ),
        create_heatmap_panel(
            "tycho_storage_state_gc_time", "time spent to gc single root"
        ),
        create_heatmap_panel(
            "tycho_storage_load_block_data_time", "Time to load block data"
        ),
        create_counter_panel(
            "tycho_storage_load_block_data_time_count",
            "Number of load_block_data calls",
        ),
        create_percent_panel(
            "tycho_storage_block_cache_hit_total",
            "tycho_storage_load_block_total",
            "Block cache hit ratio",
        ),
    ]
    return create_row("Storage", metrics)


def allocator_stats() -> RowPanel:
    metrics = [
        create_gauge_panel(
            "jemalloc_allocated_bytes", "Allocated Bytes", UNITS.BYTES_IEC
        ),
        create_gauge_panel("jemalloc_active_bytes", "Active Bytes", UNITS.BYTES_IEC),
        create_gauge_panel(
            "jemalloc_metadata_bytes", "Metadata Bytes", UNITS.BYTES_IEC
        ),
        create_gauge_panel(
            "jemalloc_resident_bytes", "Resident Bytes", UNITS.BYTES_IEC
        ),
        create_gauge_panel("jemalloc_mapped_bytes", "Mapped Bytes", UNITS.BYTES_IEC),
        create_gauge_panel(
            "jemalloc_retained_bytes", "Retained Bytes", UNITS.BYTES_IEC
        ),
        create_gauge_panel("jemalloc_dirty_bytes", "Dirty Bytes", UNITS.BYTES_IEC),
        create_gauge_panel(
            "jemalloc_fragmentation_bytes", "Fragmentation Bytes", UNITS.BYTES_IEC
        ),
    ]
    return create_row("Allocator Stats", metrics)


def templates() -> Templating:
    return Templating(
        list=[
            Template(
                name="source",
                query="prometheus",
                type="datasource",
            ),
            template(
                name="instance",
                query="label_values(tycho_net_known_peers, instance)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="workchain",
                query="label_values(tycho_do_collate_block_time_diff,workchain)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="kind",
                query="label_values(tycho_mempool_verifier_verify,kind)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
            template(
                name="method",
                query="label_values(tycho_jrpc_request_time_bucket,method)",
                data_source="${source}",
                hide=0,
                regex=None,
                multi=True,
                include_all=True,
                all_value=".*",
            ),
        ]
    )


dashboard = Dashboard(
    "Tycho Archive Node Metrics",
    templating=templates(),
    refresh="5s",
    panels=[
        stats(),
        core_block_strider(),
        storage(),
        allocator_stats(),
    ],
    annotations=Annotations(),
    uid="calaji62a1b0gb",
    version=9,
    schemaVersion=14,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    timezone="browser",
).auto_panel_ids()

# open file as stream
if len(sys.argv) > 1:
    stream = open(sys.argv[1], "w")
else:
    stream = sys.stdout
# write dashboard to file
_gen.write_dashboard(dashboard, stream)
