import datetime
import logging

from types import SimpleNamespace
from typing import Any

from google.cloud import bigquery

import apache_beam as beam
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners import PipelineState
from gfw.common.beam.pipeline.base import Pipeline

from pipe_anchorages import common as cmn
from pipe_anchorages.schema.message_schema import message_schema
from pipe_anchorages.transforms.create_tagged_anchorages import CreateTaggedAnchorages
from pipe_anchorages.transforms.sink import MessageSink
from pipe_anchorages.transforms.smart_thin_records import SmartThinRecords
from pipe_anchorages.transforms.source import QuerySource
from pipe_anchorages.utils.bqtools import BigQueryHelper, DatePartitionedTable
from pipe_anchorages.utils.ver import get_pipe_ver

logger = logging.getLogger(__name__)


def create_queries(config, start_date, end_date):
    template = """
    SELECT seg_id as ident, ssvid, lat, lon, speed,
            CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    FROM `{table}`
    WHERE DATE(timestamp) BETWEEN '{start}' AND '{end}'
      {filter_text}
    """
    start_window = start_date
    shift = 1000
    if config.ssvid_filter is None:
        filter_text = ""
    else:
        filter_core = config.ssvid_filter
        if filter_core.startswith("@"):
            with open(config.ssvid_filter[1:]) as f:
                filter_core = f.read()
        filter_text = f"AND ssvid in ({filter_core})"

    while start_window <= end_date:
        end_window = min(start_window + datetime.timedelta(days=shift), end_date)
        query = template.format(
            table=config.bq_in_messages,
            filter_text=filter_text,
            start=start_window,
            end=end_window,
        )
        yield query
        start_window = end_window + datetime.timedelta(days=1)


def anchorage_query(config):
    return f"""
    SELECT lat as anchor_lat, lon as anchor_lon, s2id as anchor_id, label
    FROM `{config.bq_in_named_anchorages}`
    """


def prepare_output_tables(config, cloud_options, start_date, end_date):
    output_table = DatePartitionedTable(
        table_id=config.bq_out_transition_messages,
        description=f"""
Created by the anchorages_pipeline: {get_pipe_ver()}.
* Creates filtered position messages flagging candidate port transitions.
* https://github.com/GlobalFishingWatch/anchorages_pipeline
* Sources: {config.bq_in_messages}
* Anchorage table: {config.bq_in_named_anchorages}
* Last processing date range: {start_date} - {end_date}
        """,
        schema=message_schema["fields"],
        partitioning_field="timestamp",
        additional_clustering_fields=["is_possible_gap_end"],
    )

    bq_helper = BigQueryHelper(
        bq_client=bigquery.Client(
            project=cloud_options.project,
        ),
        labels=dict([entry.split("=") for entry in cloud_options.labels]),
    )

    bq_helper.ensure_table_exists(output_table)
    bq_helper.update_table(output_table)
    # Ensure we delete any existing rows in the date range to be processed.
    # Needed to maintain consistency if are re-processing past dates.
    logger.info("Deleting existing rows in the range [{}-{}]".format(start_date, end_date))
    bq_helper.run_query(output_table.clear_query(start_date, end_date))


def run(config: SimpleNamespace, **kwargs: Any) -> int:
    gfw_pipeline = Pipeline(
        unparsed_args=config.unknown_unparsed_args,
        labels=[f"{key}={value}" for key, value in (config.labels or {}).items()],
        **config.unknown_parsed_args,
        **kwargs,
    )
    cloud_options = gfw_pipeline.cloud_options

    start_date = datetime.datetime.strptime(config.start_date, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(config.end_date, "%Y-%m-%d").date()

    p = gfw_pipeline.pipeline

    # Ensure that S2 Cell sizes are large enough that we don't miss ports.
    anchorage_visit_max_distance = max(
        config.anchorage_entry_dist_km, config.anchorage_exit_dist_km
    )
    assert anchorage_visit_max_distance * cmn.VISIT_SAFETY_FACTOR < 2 * cmn.approx_visit_cell_size

    queries = create_queries(config, start_date, end_date)

    sources = [
        (p | f"Read_{i}" >> QuerySource(query, cloud_options)) for (i, query) in enumerate(queries)
    ]

    tagged_records = (
        sources
        | beam.Flatten()
        | cmn.CreateVesselRecords(destination=None)
        | cmn.CreateTaggedRecordsByDay()
    )

    anchorages = (
        p
        | "ReadAnchorages" >> QuerySource(anchorage_query(config), cloud_options)
        | CreateTaggedAnchorages()
    )

    _ = (
        tagged_records
        | "thinRecords" >> SmartThinRecords(
            anchorages=anchorages,
            anchorage_entry_dist=config.anchorage_entry_dist_km,
            anchorage_exit_dist=config.anchorage_exit_dist_km,
            stopped_begin_speed=config.stopping_speed_knots,
            stopped_end_speed=config.starting_speed_knots,
            min_gap_minutes=config.min_anchorage_gap_minutes,
            start_date=start_date,
            end_date=end_date,
        )
        | "writeThinnedRecords" >> MessageSink(config.bq_out_transition_messages)
    )

    prepare_output_tables(config, cloud_options, start_date, end_date)
    result = p.run()

    success_states = set(
        [
            PipelineState.DONE,
            PipelineState.RUNNING,
            PipelineState.UNKNOWN,
            PipelineState.PENDING,
        ]
    )

    runner = gfw_pipeline.pipeline_options.view_as(StandardOptions).runner
    if config.wait_for_job or runner == "DirectRunner":
        result.wait_until_finish()

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
