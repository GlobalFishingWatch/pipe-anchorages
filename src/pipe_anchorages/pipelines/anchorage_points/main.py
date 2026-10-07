import datetime
import logging

from types import SimpleNamespace
from typing import Any, Callable

import apache_beam as beam
from apache_beam.runners import PipelineState

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.transforms import ReadFromBigQuery, WriteToBigQueryWrapper

from pipe_anchorages import common as cmn
from pipe_anchorages.find_anchorage_points import FindAnchoragePoints
from pipe_anchorages.records import VesselLocationRecord
from pipe_anchorages.pipelines.anchorage_points.config import AnchoragePointsConfig
from pipe_anchorages.pipelines.anchorage_points.table_config import (
    AnchoragePointsTableConfig,
    AnchoragePointsTableDescription,
)
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


def has_location_record(item):
    _, rcd = item
    return isinstance(rcd, VesselLocationRecord)


def encode_anchorage(anchorage) -> dict:
    return {
        "lat": anchorage.mean_location.lat,
        "lon": anchorage.mean_location.lon,
        "total_visits": anchorage.total_visits,
        "drift_radius": anchorage.rms_drift_radius,
        "top_destination": anchorage.top_destination,
        "unique_stationary_ssvid": len(anchorage.vessels),
        "unique_stationary_fishing_ssvid": len(anchorage.fishing_vessels),
        "unique_active_ssvid": anchorage.active_ssvids,
        "unique_total_ssvid": anchorage.total_ssvids,
        "active_ssvid_days": anchorage.active_ssvid_days,
        "stationary_ssvid_days": anchorage.stationary_ssvid_days,
        "stationary_fishing_ssvid_days": anchorage.stationary_fishing_ssvid_days,
        "s2id": anchorage.s2id,
    }


class AnchoragePointsCore(beam.PTransform):
    """Turns position messages into anchorage points.

    Groups run()'s inline chain (CreateVesselRecords -> filter location records ->
    CreateTaggedRecords -> FindAnchoragePoints) into one composite PTransform, since
    LinearDag's core slot takes exactly one transform. The fishing vessel list is read
    here too, as a side input of FindAnchoragePoints -- LinearDag's own side_inputs
    slot would require implementing set_side_inputs, which we don't need yet.
    """

    def __init__(self, config: AnchoragePointsConfig) -> None:
        self.min_positions = config.min_positions
        self.min_duration = datetime.timedelta(
            minutes=config.stationary_period_min_duration_minutes
        )
        self.max_distance_km = config.stationary_period_max_distance_km
        self.min_unique_vessels = config.min_unique_vessels
        self.gcs_in_fishing_ssvids = config.gcs_in_fishing_ssvids

    def expand(self, xs):
        fishing_vessels = xs.pipeline | "ReadFishingVessels" >> beam.io.ReadFromText(
            self.gcs_in_fishing_ssvids
        )
        fishing_vessel_list = beam.pvalue.AsList(fishing_vessels)

        return (
            xs
            | cmn.CreateVesselRecords()
            | "FilterOutInfo" >> beam.Filter(has_location_record)
            | cmn.CreateTaggedRecords(self.min_positions)
            | FindAnchoragePoints(
                self.min_duration,
                self.max_distance_km,
                self.min_unique_vessels,
                fishing_vessel_list,
            )
        )


class AnchoragePointsSink(beam.PTransform):
    """Encodes AnchoragePoints and writes them to BigQuery.

    Takes the same WriteToBigQuery-like factory that gfw-common's
    WriteToBigQueryWrapper takes, so tests can inject a fake BigQuery client
    (--mock-bq-clients) instead of a real one.
    """

    def __init__(
        self,
        table_config: AnchoragePointsTableConfig,
        factory: Callable = WriteToBigQueryWrapper.get_client_factory(),
    ) -> None:
        self._table_config = table_config
        self._factory = factory

    def expand(self, xs):
        return xs | beam.Map(encode_anchorage) | WriteToBigQueryWrapper(
            table=self._table_config.table_id,
            schema=self._table_config.schema,
            write_to_bigquery_factory=self._factory,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            additional_bq_parameters={
                "destinationTableProperties": {
                    "description": self._table_config.description.render()
                },
            },
        )


def run(config: SimpleNamespace, **kwargs: Any) -> int:
    config = AnchoragePointsConfig.from_namespace(config, version=__version__)

    read_factory = ReadFromBigQuery.get_client_factory(mocked=bool(config.mock_bq_clients))
    write_factory = WriteToBigQueryWrapper.get_client_factory(mocked=bool(config.mock_bq_clients))

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
                read_from_bigquery_factory=read_factory,
                read_from_bigquery_kwargs={"bigquery_job_labels": config.labels or {}},
            )
            for i, query in enumerate(create_queries(config))
        ],
        core=AnchoragePointsCore(config),
        sinks=(AnchoragePointsSink(table_config, write_factory),),
    )

    pipeline = Pipeline(
        name="pipe-anchorages",
        version=__version__,
        dag=dag,
        unparsed_args=config.unknown_unparsed_args,
        labels=config.labels or None,
        **config.unknown_parsed_args,
        **kwargs,
    )

    result, _ = pipeline.run()

    success_states = set(
        [PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING]
    )

    logger.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
