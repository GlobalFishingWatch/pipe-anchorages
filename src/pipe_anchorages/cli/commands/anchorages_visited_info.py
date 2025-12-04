from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_anchorages.pipelines.anchorages_visited_info.main import run


DESCRIPTION = """\
Generates an anchorages reference table with boolean cross-events presence indicators.

You can provide a configuration file or command-line arguments.
The latter take precedence, so if you provide both, command-line arguments
will overwrite options in the config file provided.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_BQ_INPUT_LOITERING = "BigQuery table with with loitering events."
HELP_BQ_INPUT_ENCOUNTERS = "BigQuery table with with encounter events."
HELP_BQ_INPUT_AIS_GAPS = "BigQuery table with ais gap events."
HELP_BQ_INPUT_NAMED_ANCHORAGES = "BigQuery table with named anchorages dataset."
HELP_BQ_OUTPUT = "BigQuery table in which to store the anchorages visited info dataset."

HELP_MOCK_BQ_CLIENTS = "If passed, mocks the BQ clients [Useful for development]."
HELP_BQ_PROJECT = "Project to use when executing the events query."
HELP_DRY_RUN = "If True, executes queries in dry run mode."


class AnchoragesVisitedInfo(Command):
    @property
    def name(cls):
        return "anchorages-visited-info"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--bq-input-loitering", type=str, help=HELP_BQ_INPUT_LOITERING),
            Option("--bq-input-encounters", type=str, help=HELP_BQ_INPUT_ENCOUNTERS),
            Option("--bq-input-ais-gaps", type=str, help=HELP_BQ_INPUT_AIS_GAPS),
            Option("--bq-input-named-anchorages", type=str, help=HELP_BQ_INPUT_NAMED_ANCHORAGES),
            Option("--bq-output", type=str, help=HELP_BQ_OUTPUT),
            Option("--mock-bq-clients", type=bool, help=HELP_MOCK_BQ_CLIENTS),
            Option("--project", type=str, help=HELP_BQ_PROJECT),
            Option("--dry-run", type=bool, help=HELP_DRY_RUN),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        run(config, **kwargs)
