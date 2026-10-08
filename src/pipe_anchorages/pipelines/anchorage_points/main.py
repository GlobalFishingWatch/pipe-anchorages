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
from pipe_anchorages.queries.anchorage_points import AnchoragePointsQuery
from pipe_anchorages.version import __version__

logger = logging.getLogger(__name__)


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
            ReadFromBigQuery.from_query(
                AnchoragePointsQuery(
                    source_messages=config.bq_in_messages,
                    start_date=config.start_date,
                    end_date=config.end_date,
                ).with_env(config.jinja_env),
                label="ReadMessages",
                read_from_bigquery_factory=read_from_bigquery_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
            ),
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
