"""This module implements a CLI for the anchorages pipeline."""
import sys
import logging

from gfw.common.logging import LoggerConfig
from gfw.common.cli import CLI, Option
from gfw.common.cli.actions import NestedKeyValueAction
from gfw.common.cli.formatting import default_formatter

from pipe_anchorages.version import __version__
from pipe_anchorages.cli.commands import AnchoragesVisitedInfo


logger = logging.getLogger(__name__)


NAME = "pipe-anchorages"
DESCRIPTION = "Tools for finding anchorages and associated port-visit events."
HELP_LABELS = "Labels to audit costs over the queries."


def run(args):
    gaps_cli = CLI(
        name=NAME,
        description=DESCRIPTION,
        formatter=default_formatter(max_pos=120),
        subcommands=[
            AnchoragesVisitedInfo,
        ],
        options=[  # Common options for all subcommands.
            Option(
                "--labels", type=str, nargs="*", action=NestedKeyValueAction, help=HELP_LABELS
            ),
        ],
        version=__version__,
        examples=[
            "pipe-anchorages anchorages-visited-info -c config/sample-anchorages-visited.json",
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

    return gaps_cli.execute(args)


def main():
    run(sys.argv[1:])


# FROM NOW ON: LEGACY ENTRY POINT.
# TODO: REMOVE AFTER MIGRATING THE REST OF THE COMMANDS.

def run_generate_confidence_voyages(args):
    from pipe_anchorages.confidence_voyages import run as run_confidence_voyages
    run_confidence_voyages(args)


def run_thin_port_messages(args):
    from pipe_anchorages.thin_port_messages import run as run_thin_port_messages
    run_thin_port_messages(args)


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
    "thin_port_messages": run_thin_port_messages,
    "port_visits": run_port_visits,
    "anchorages": run_anchorages,
    "name_anchorages": run_name_anchorages,
    "generate_confidence_voyages": run_generate_confidence_voyages,
    "anchorages_visited_info": lambda args: run(["anchorages-visited-info"] + args)
}


def main_legacy():
    logging.basicConfig(level=logging.INFO)
    logging.info("Running %s", sys.argv)

    if len(sys.argv) < 2:
        logging.info(
            "No subcommand specified. Run pipeline [SUBCOMMAND], where subcommand is one of %s",
            SUBCOMMANDS.keys(),
        )
        exit(1)

    subcommand = sys.argv[1]
    subcommand_args = sys.argv[2:]

    SUBCOMMANDS[subcommand](subcommand_args)


if __name__ == "__main__":
    main()
