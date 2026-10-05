from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_anchorages import anchorages_pipeline


DESCRIPTION = """\
Finds candidate anchorage points by clustering vessel positions where they
remain stationary near the coast.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_IN_MESSAGES = "BigQuery table to pull position messages from."
HELP_IN_SEGMENTS = "BigQuery table with segment destinations, partitioned by day."
HELP_OUT_POINTS = "BigQuery table in which to store the anchorage points."
HELP_START_DATE = "First date to look for stationary positions."
HELP_END_DATE = "Last date (exclusive) to look for stationary positions."
HELP_FISHING_SSVIDS = "Newline-separated list of fishing vessel ids."
HELP_MIN_POSITIONS = "Minimum number of positions a segment needs to be considered."
HELP_STATIONARY_PERIOD_MIN_DURATION_MINUTES = (
    "Minimum time (minutes) a vessel must stay within the stationary radius to count."
)
HELP_STATIONARY_PERIOD_MAX_DISTANCE_KM = (
    "Max drift radius (km) from a position while still considered stationary there."
)
HELP_MIN_UNIQUE_VESSELS_FOR_ANCHORAGE = (
    "Minimum number of distinct vessels that must visit a cluster for it to count as an anchorage."
)


class AnchoragePoints(Command):

    @property
    def name(self):
        return "anchorage-points"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--bq-in-messages", type=str, required=True, help=HELP_IN_MESSAGES),
            Option("--bq-in-segments", type=str, required=True, help=HELP_IN_SEGMENTS),
            Option("--bq-out-anchorage-points", type=str, required=True, help=HELP_OUT_POINTS),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--gcs-in-fishing-ssvids", type=str, required=True, help=HELP_FISHING_SSVIDS),
            Option("--min-positions", type=int, default=200, help=HELP_MIN_POSITIONS),
            Option(
                "--stationary-period-min-duration-minutes",
                type=int,
                default=720,
                help=HELP_STATIONARY_PERIOD_MIN_DURATION_MINUTES,
            ),
            Option(
                "--stationary-period-max-distance-km",
                type=float,
                default=0.5,
                help=HELP_STATIONARY_PERIOD_MAX_DISTANCE_KM,
            ),
            Option(
                "--min-unique-vessels-for-anchorage",
                type=int,
                default=20,
                help=HELP_MIN_UNIQUE_VESSELS_FOR_ANCHORAGE,
            ),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return anchorages_pipeline.run(config, **kwargs)
