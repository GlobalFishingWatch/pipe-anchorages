import datetime
import logging

from types import SimpleNamespace
from typing import Any

import apache_beam as beam
from apache_beam.runners import PipelineState
from gfw.common.beam.pipeline.base import Pipeline

from pipe_anchorages import common as cmn
from pipe_anchorages.find_anchorage_points import FindAnchoragePoints
from pipe_anchorages.records import VesselLocationRecord
from pipe_anchorages.transforms.sink import AnchorageSink
from pipe_anchorages.transforms.source import QuerySource


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


def run(config: SimpleNamespace, **kwargs: Any) -> int:
    gfw_pipeline = Pipeline(
        unparsed_args=config.unknown_unparsed_args,
        labels=[f"{key}={value}" for key, value in (config.labels or {}).items()],
        **config.unknown_parsed_args,
        **kwargs,
    )
    cloud_options = gfw_pipeline.cloud_options

    queries = create_queries(config)

    p = gfw_pipeline.pipeline

    fishing_vessels = p | beam.io.ReadFromText(config.fishing_ssvid_list)
    fishing_vessel_list = beam.pvalue.AsList(fishing_vessels)

    source = [
        (p | f"Source_{i}" >> QuerySource(query, cloud_options))
        for (i, query) in enumerate(queries)
    ] | beam.Flatten()

    tagged_records = (
        source
        | cmn.CreateVesselRecords()
        | "FilterOutInfo" >> beam.Filter(has_location_record)
        | cmn.CreateTaggedRecords(config.min_required_positions)
    )

    anchorage_points = tagged_records | FindAnchoragePoints(
        datetime.timedelta(minutes=config.stationary_period_min_duration_minutes),
        config.stationary_period_max_distance_km,
        config.min_unique_vessels_for_anchorage,
        fishing_vessel_list,
    )

    (anchorage_points | AnchorageSink(config.bq_out_anchorage_points, config, cloud_options))

    result = p.run()

    success_states = set(
        [PipelineState.DONE, PipelineState.RUNNING, PipelineState.UNKNOWN, PipelineState.PENDING]
    )

    logging.info("returning with result.state=%s" % result.state)
    return 0 if result.state in success_states else 1
