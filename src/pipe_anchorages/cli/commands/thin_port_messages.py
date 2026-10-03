from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_anchorages import thin_port_messages_pipeline
from pipe_anchorages.beam_options import build_pipeline_options
from pipe_anchorages.options.thin_port_messages_options import default_config_file


DESCRIPTION = """\
Thins raw position messages near named anchorages into entry/exit events, ahead
of port-visit detection.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_BQ_IN_NAMED_ANCHORAGES = "BigQuery table with named anchorages."
HELP_BQ_IN_MESSAGES = "BigQuery table to pull position messages from."
HELP_BQ_OUT_PORT_EVENTS = "BigQuery table in which to store the thinned port events."
HELP_START_DATE = "First date to look for entry/exit events."
HELP_END_DATE = "Last date (inclusive) to look for entry/exit events."
HELP_CONFIG = "Path to the pipeline parameters file."
HELP_SSVID_FILTER = (
    "Subquery or list of ssvid to limit processing to. If prefixed by @, load from given path."
)
HELP_WAIT_FOR_JOB = "Wait until the job finishes before returning."

# This command's own domain-specific option dests, fed to build_pipeline_options().
# Deliberately excludes Beam/Dataflow-native flags (--runner, --project, ...), which
# aren't declared here at all so they pass through as unknown args instead.
FIELDS = [
    "bq_in_named_anchorages",
    "bq_in_messages",
    "bq_out_port_events",
    "start_date",
    "end_date",
    "config",
    "ssvid_filter",
    "wait_for_job",
]


class ThinPortMessages(Command):

    @property
    def name(self):
        return "thin-port-messages"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option(
                "--bq-in-named-anchorages",
                type=str,
                required=True,
                help=HELP_BQ_IN_NAMED_ANCHORAGES,
            ),
            Option("--bq-in-messages", type=str, required=True, help=HELP_BQ_IN_MESSAGES),
            Option("--bq-out-port-events", type=str, required=True, help=HELP_BQ_OUT_PORT_EVENTS),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--config", type=str, default=str(default_config_file), help=HELP_CONFIG),
            Option("--ssvid-filter", type=str, help=HELP_SSVID_FILTER),
            Option("--wait-for-job", type=bool, help=HELP_WAIT_FOR_JOB),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        options = build_pipeline_options(config, FIELDS)
        return thin_port_messages_pipeline.run(options)
