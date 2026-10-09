import datetime
import logging
from datetime import date
from functools import cached_property
from types import SimpleNamespace
from typing import Any, Callable, NamedTuple

import apache_beam as beam
from apache_beam.runners import PipelineState

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.pipeline.hooks import create_table_hook
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper
from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_anchorages.hooks import update_table_metadata_hook
from pipe_anchorages.pipelines.anchorage_locations.config import AnchorageLocationsConfig
from pipe_anchorages.pipelines.anchorage_locations.table_config import (
    AnchorageLocationsTableConfig,
    AnchorageLocationsTableDescription,
)
from pipe_anchorages.pipelines.anchorage_locations.transforms.core import FindAnchorageLocations
from pipe_anchorages.version import __version__

logger = logging.getLogger(__name__)


class AnchorageMessage(NamedTuple):
    """Output type of the anchorage position messages query."""

    ident: str
    lat: float
    lon: float
    timestamp: float
    destination: str
    speed: float


class MessagesQuery(Query):
    """Encapsulates the position messages query."""

    NAME = "messages"
    JINJA_TEMPLATE_FILENAME = "messages.sql.j2"

    def __init__(self, source_messages: str, start_date: date, end_date: date):
        self._source_messages = source_messages
        self._start_date = start_date
        self._end_date = end_date

    @cached_property
    def output_type(self) -> type[NamedTuple]:
        return AnchorageMessage

    @cached_property
    def template_filename(self) -> str:
        return self.JINJA_TEMPLATE_FILENAME

    @cached_property
    def template_vars(self) -> dict:
        return {
            "source_messages": self._source_messages,
            "start_date": self._start_date,
            "end_date": self._end_date,
        }


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    read_from_bigquery_factory: Callable = None,
    write_to_bigquery_factory: Callable = None,
    bq_client_factory: Callable = None,
    **kwargs: Any,
) -> int:
    config = AnchorageLocationsConfig.from_namespace(config, version=__version__)

    if read_from_bigquery_factory is None:
        read_from_bigquery_factory = ReadFromBigQuery.get_client_factory(
            mocked=config.mock_bq_clients
        )
    if write_to_bigquery_factory is None:
        write_to_bigquery_factory = WriteToBigQueryWrapper.get_client_factory(
            mocked=config.mock_bq_clients
        )
    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(mocked=config.mock_bq_clients)

    table_config = AnchorageLocationsTableConfig(
        table_id=config.bq_out_anchorage_locations,
        description=AnchorageLocationsTableDescription(
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
                MessagesQuery(
                    source_messages=config.bq_in_messages,
                    start_date=config.start_date,
                    end_date=config.end_date,
                ).with_env(config.jinja_env),
                label="ReadMessages",
                read_from_bigquery_factory=read_from_bigquery_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels},
            ),
        ],
        core=FindAnchorageLocations(
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
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            ),
        ),
    )

    pipeline = Pipeline(
        name="pipe-anchorages",
        version=__version__,
        dag=dag,
        pre_hooks=[create_table_hook(table_config, mock=config.mock_bq_clients)],
        post_hooks=[update_table_metadata_hook(table_config, config.labels, bq_client_factory)],
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
