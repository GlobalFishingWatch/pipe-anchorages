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
HELP_OUT_ANCHORAGES = "BigQuery table in which to store the anchorage points."
HELP_START_DATE = "First date to look for stationary positions."
HELP_END_DATE = "Last date (exclusive) to look for stationary positions."
HELP_CONFIG = "Path to the pipeline parameters file."
HELP_FISHING_SSVID_LIST = "GCS location of a newline-separated list of fishing vessel ids."


class Anchorages(Command):

    @property
    def name(self):
        return "anchorages"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--bq-in-messages", type=str, required=True, help=HELP_IN_MESSAGES),
            Option("--bq-in-segments", type=str, required=True, help=HELP_IN_SEGMENTS),
            Option("--bq-out-anchorages", type=str, required=True, help=HELP_OUT_ANCHORAGES),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--config", type=str, required=True, help=HELP_CONFIG),
            Option(
                "--fishing-ssvid-list", type=str, required=True, help=HELP_FISHING_SSVID_LIST
            ),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return anchorages_pipeline.run(config, **kwargs)
