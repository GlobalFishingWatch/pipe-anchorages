"""This module implements a CLI for the anchorages pipeline."""
import sys
import logging

from gfw.common.logging import LoggerConfig
from gfw.common.cli import CLI, Option
from gfw.common.cli.actions import NestedKeyValueAction
from gfw.common.cli.formatting import default_formatter

from pipe_anchorages.version import __version__
from pipe_anchorages.cli.commands import (
    AnchoragesVisitedInfo,
    ConfidenceVoyages,
    PortTransitionMessages,
)


logger = logging.getLogger(__name__)


NAME = "pipe-anchorages"
DESCRIPTION = "Tools for finding anchorages and associated port-visit events."
HELP_LABELS = "Labels to audit costs over the queries."


def run(args):
    cli = CLI(
        name=NAME,
        description=DESCRIPTION,
        formatter=default_formatter(max_pos=120),
        subcommands=[
            AnchoragesVisitedInfo,
            ConfidenceVoyages,
            PortTransitionMessages,
        ],
        options=[  # Common options for all subcommands.
            Option(
                "--labels", type=str, nargs="*", action=NestedKeyValueAction, help=HELP_LABELS
            ),
        ],
        version=__version__,
        examples=[
            "pipe-anchorages anchorages-visited-info -c config/sample-anchorages-visited.json",
            "pipe-anchorages confidence-voyages "
            "--bq-in-port-visits project.dataset.port_visits --min-confidence 3 "
            "--bq-out-voyages project.dataset.voyages_c3 --project world-fishing-827",
            "pipe-anchorages port-transition-messages "
            "--bq-in-named-anchorages project.dataset.anchorages "
            "--bq-in-messages project.dataset.messages "
            "--bq-out-port-transition-messages project.dataset.output "
            "--start-date 2024-01-01 --end-date 2024-01-07",
        ],
        logger_config=LoggerConfig(
            warning_level=[
                "apache_beam.runners.portability",
                "apache_beam.runners.worker",
                "apache_beam.transforms.core",
                "apache_beam.io.filesystem",
                "apache_beam.io.gcp.bigquery_tools",
                "urllib3"
            ]
        ),
        allow_unknown=True
    )

    return cli.execute(args)


# FROM NOW ON: LEGACY ENTRY POINT.
# TODO: REMOVE AFTER MIGRATING THE REST OF THE COMMANDS.

def run_port_visits(args):
    from pipe_anchorages.port_visits import run as run_port_visits
    run_port_visits(args)


def run_anchorages(args):
    from pipe_anchorages.anchorages import run as run_anchorages
    run_anchorages(args)


def run_name_anchorages(args):
    from pipe_anchorages.name_anchorages import run as run_name_anchorages
    run_name_anchorages(args)


SUBCOMMANDS = {
    "port_visits": run_port_visits,
    "anchorages": run_anchorages,
    "name_anchorages": run_name_anchorages,
    "anchorages_visited_info": lambda args: run(["anchorages-visited-info"] + args),
    "generate_confidence_voyages": lambda args: run(["confidence-voyages"] + args),
    "thin_port_messages": lambda args: run(["port-transition-messages"] + args),
}


def main():
    # This is the only line needed after the rest of the CLI commands are migrated.
    # run(sys.argv[1:])

    # TODO: Remove the following after the commands are migrated.
    # Only the still-unmigrated legacy commands need the old dispatch; anything else
    # (new-style kebab-case command names, --help, --version, ...) goes through the
    # real framework, which already handles usage/errors on its own.
    args = sys.argv[1:]

    if args and args[0] in SUBCOMMANDS:
        SUBCOMMANDS[args[0]](args[1:])
    else:
        run(args)


if __name__ == "__main__":
    main()
