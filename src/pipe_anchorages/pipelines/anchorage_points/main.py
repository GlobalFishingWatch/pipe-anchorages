import datetime
import logging

from types import SimpleNamespace
from typing import Any, Callable

import apache_beam as beam
from apache_beam.runners import PipelineState

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper


from pipe_anchorages.pipelines.anchorage_points.config import AnchoragePointsConfig
from pipe_anchorages.pipelines.anchorage_points.table_config import (
    AnchoragePointsTableConfig,
    AnchoragePointsTableDescription,
)
from pipe_anchorages.pipelines.anchorage_points.transforms.core import FindAnchoragePoints
from pipe_anchorages.version import __version__

logger = logging.getLogger(__name__)


def create_queries(config):
    template = """
    WITH

    destinations AS (
      SELECT seg_id, _TABLE_SUFFIX AS table_suffix,
          CASE
            WHEN ARRAY_LENGTH(destinations) = 0 THEN NULL
            ELSE (SELECT MAX(value)
                  OVER (ORDER BY count DESC)
                  FROM UNNEST(destinations)
                  LIMIT 1)
            END AS destination
      FROM `{segment_table}*`
      WHERE _TABLE_SUFFIX BETWEEN '{start:%Y%m%d}' AND '{end:%Y%m%d}'
    ),

    positions AS (
      SELECT ssvid, seg_id, lat, lon, timestamp, speed,
             date(timestamp) as table_suffix
        FROM `{position_table}`
       WHERE date(timestamp) BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}'
         AND seg_id IS NOT NULL
         AND lat IS NOT NULL
         AND lon IS NOT NULL
         AND speed IS NOT NULL
    )

    SELECT ssvid as ident,
           lat,
           lon,
           timestamp,
           destination,
           speed
    FROM positions
    JOIN destinations
    USING (seg_id, table_suffix)
    """
    start_window = datetime.datetime.strptime(config.start_date, "%Y-%m-%d")
    end_window = datetime.datetime.strptime(config.end_date, "%Y-%m-%d")

    queries = []
    start = start_window
    while start < end_window:
        # Add 999 days so that we get 1000 total days
        end = min(start + datetime.timedelta(days=999), end_window)
        queries.append(
            template.format(
                position_table=config.bq_in_messages,
                segment_table=config.bq_in_segments,
                start=start,
                end=end,
            )
        )
        # Add 1 day to end, so that we don't overlap.
        start = end + datetime.timedelta(days=1)

    return queries


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    read_from_bigquery_factory: Callable = None,
    write_to_bigquery_factory: Callable = None,
    **kwargs: Any,
) -> int:
    config = AnchoragePointsConfig.from_namespace(config, version=__version__)

    if read_from_bigquery_factory is None:
        read_from_bigquery_factory = ReadFromBigQuery.get_client_factory(
            mocked=bool(config.mock_bq_clients)
        )
    if write_to_bigquery_factory is None:
        write_to_bigquery_factory = WriteToBigQueryWrapper.get_client_factory(
            mocked=bool(config.mock_bq_clients)
        )

    table_config = AnchoragePointsTableConfig(
        table_id=config.bq_out_anchorage_points,
        description=AnchoragePointsTableDescription(
            version=__version__,
            relevant_params={
                "bq_in_messages": config.bq_in_messages,
                "bq_in_segments": config.bq_in_segments,
                "start_date": config.start_date,
                "end_date": config.end_date,
                "min_positions": config.min_positions,
                "min_unique_vessels": config.min_unique_vessels,
                "stationary_period_min_duration_minutes": (
                    config.stationary_period_min_duration_minutes
                ),
                "stationary_period_max_distance_km": config.stationary_period_max_distance_km,
            },
        ),
    )

    dag = LinearDag(
        sources=[
            ReadFromBigQuery(
                query=query,
                label=f"Source_{i}",
                read_from_bigquery_factory=read_from_bigquery_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
            )
            for i, query in enumerate(create_queries(config))
        ],
        core=FindAnchoragePoints(
            min_positions=config.min_positions,
            min_duration=datetime.timedelta(
                minutes=config.stationary_period_min_duration_minutes
            ),
            max_distance_km=config.stationary_period_max_distance_km,
            min_unique_vessels=config.min_unique_vessels,
        ),
        side_inputs=beam.io.ReadFromText(config.gcs_in_fishing_ssvids),
        sinks=(
            WriteToBigQueryWrapper(
                table=table_config.table_id,
                schema=table_config.schema,
                write_to_bigquery_factory=write_to_bigquery_factory,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                additional_bq_parameters={
                    "destinationTableProperties": {
                        "description": table_config.description.render()
                    },
                },
            ),
        ),
    )

    pipeline = Pipeline(
        name="pipe-anchorages",
        version=__version__,
        dag=dag,
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels,
        **config.unknown_parsed_args,
        **kwargs,
    )

    result, _ = pipeline.run()

    success_states = {
        PipelineState.DONE,
        PipelineState.RUNNING,
        PipelineState.UNKNOWN,
        PipelineState.PENDING,
    }

    logger.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
