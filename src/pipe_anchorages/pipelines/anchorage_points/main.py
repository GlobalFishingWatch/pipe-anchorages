import datetime
import logging

from types import SimpleNamespace
from typing import Any

import apache_beam as beam
from apache_beam.runners import PipelineState

from gfw.common.beam.pipeline.base import Pipeline
from gfw.common.beam.pipeline.dag import LinearDag

from pipe_anchorages import common as cmn
from pipe_anchorages.find_anchorage_points import FindAnchoragePoints
from pipe_anchorages.records import VesselLocationRecord
from pipe_anchorages.pipelines.anchorage_points.config import AnchoragePointsConfig
from pipe_anchorages.transforms.sink import AnchorageSink
from pipe_anchorages.transforms.source import QuerySource
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


def run(config: SimpleNamespace, **kwargs: Any) -> int:
    config = AnchoragePointsConfig.from_namespace(config, version=__version__)

    pipeline = Pipeline(
        unparsed_args=config.unknown_unparsed_args,
        # Beam's GoogleCloudOptions wants labels as ["key=value"]; handing it the
        # CLI's dict makes it json-stringify the whole thing, which later crashes
        # cloud_to_labels() (no "=" to split on).
        labels=[f"{key}={value}" for key, value in config.labels.items()]
        if config.labels
        else None,
        **config.unknown_parsed_args,
        **kwargs,
    )
    cloud_options = pipeline.cloud_options

    dag = LinearDag(
        sources=[
            f"Source_{i}" >> QuerySource(query, cloud_options)
            for i, query in enumerate(create_queries(config))
        ],
        core=AnchoragePointsCore(config),
        sinks=(AnchorageSink(config.bq_out_anchorage_points, config, cloud_options),),
    )
    dag.apply(pipeline.pipeline)

    result = pipeline.pipeline.run()

    success_states = set(
        [PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING]
    )

    logger.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
