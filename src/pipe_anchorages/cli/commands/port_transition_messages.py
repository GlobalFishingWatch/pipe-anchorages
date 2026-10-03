from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_anchorages import thin_port_messages_pipeline


DESCRIPTION = """\
Flags candidate port-related state transitions (entering/exiting an anchorage,
stopping/starting to move within one) from raw position messages near named
anchorages. This is an intermediate step, not a finished product: the output
rows are still shaped like position messages, not discrete events -- they're
just the minimal subset needed to reconstruct where a transition happened.

This command processes positions in bounded windows (currently per segment,
per day), not each vessel's full history at once. Because of that, it can't
always tell on its own whether a transition or a tracking gap happened right
at the edge of its own window -- it conservatively keeps the boundary records
either way, and port-visits (which sees each vessel's complete history) is
what resolves those boundary cases and assembles the final visit episodes.

Besides the arguments defined here, you can also pass any pipeline option
defined for Apache Beam PipelineOptions class. For more information, see
    https://cloud.google.com/dataflow/docs/reference/pipeline-options#python.\n
"""

HELP_BQ_IN_NAMED_ANCHORAGES = "BigQuery table with named anchorages."
HELP_BQ_IN_MESSAGES = "BigQuery table to pull position messages from."
HELP_BQ_OUT_PORT_TRANSITION_MESSAGES = "BigQuery table in which to store the filtered messages."
HELP_START_DATE = "First date to look for entry/exit events."
HELP_END_DATE = "Last date (inclusive) to look for entry/exit events."
HELP_SSVID_FILTER = (
    "Subquery or list of ssvid to limit processing to. If prefixed by @, load from given path."
)
HELP_WAIT_FOR_JOB = "Wait until the job finishes before returning."
HELP_ANCHORAGE_ENTRY_DISTANCE_KM = "Max distance (km) from an anchorage to count as an entry."
HELP_ANCHORAGE_EXIT_DISTANCE_KM = "Min distance (km) from an anchorage to count as an exit."
HELP_STOPPED_BEGIN_SPEED_KNOTS = "Speed (knots) below which a vessel is considered stopped."
HELP_STOPPED_END_SPEED_KNOTS = "Speed (knots) above which a stopped vessel is moving again."
HELP_MINIMUM_PORT_GAP_DURATION_MINUTES = (
    "Minimum gap (minutes) between two records for them to count as separate port visits."
)


class PortTransitionMessages(Command):

    @property
    def name(self):
        return "port-transition-messages"

    @property
    def description(self):
        return DESCRIPTION

    @staticmethod
    def transition_options() -> list:
        """Options shared with port-visits, which applies the same state machine.

        Owned here since this command runs first in the pipeline; port-visits
        imports this method rather than redeclaring the same 5 Options, so the
        two can never drift apart.
        """
        return [
            Option(
                "--anchorage-entry-distance-km",
                type=float,
                default=3.0,
                help=HELP_ANCHORAGE_ENTRY_DISTANCE_KM,
            ),
            Option(
                "--anchorage-exit-distance-km",
                type=float,
                default=4.0,
                help=HELP_ANCHORAGE_EXIT_DISTANCE_KM,
            ),
            Option(
                "--stopped-begin-speed-knots",
                type=float,
                default=0.2,
                help=HELP_STOPPED_BEGIN_SPEED_KNOTS,
            ),
            Option(
                "--stopped-end-speed-knots",
                type=float,
                default=0.5,
                help=HELP_STOPPED_END_SPEED_KNOTS,
            ),
            Option(
                "--minimum-port-gap-duration-minutes",
                type=float,
                default=240.0,
                help=HELP_MINIMUM_PORT_GAP_DURATION_MINUTES,
            ),
        ]

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
            Option(
                "--bq-out-port-transition-messages",
                type=str,
                required=True,
                help=HELP_BQ_OUT_PORT_TRANSITION_MESSAGES,
            ),
            Option("--start-date", type=str, required=True, help=HELP_START_DATE),
            Option("--end-date", type=str, required=True, help=HELP_END_DATE),
            Option("--ssvid-filter", type=str, help=HELP_SSVID_FILTER),
            Option("--wait-for-job", type=bool, default=False, help=HELP_WAIT_FOR_JOB),
            *self.transition_options(),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return thin_port_messages_pipeline.run(config, **kwargs)
