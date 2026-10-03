from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_anchorages.confidence_voyages import run


DESCRIPTION = """\
Generates the confidence voyages table for a given minimum confidence level.

A "voyage" is the combination of a vessel's previous port_visit's end and next
port_visit's start.
"""

HELP_SOURCE = "The BQ source table (Format str, ex: dataset.table)."
HELP_MIN_CONFIDENCE = "The minimal confidence to detect the voyages (Format str, ex: 3)."
HELP_OUTPUT = "The BQ destination table (Format str, ex: project.dataset.table)."
HELP_PROJECT = "The GCP project billed for the processing of this step."


class ConfidenceVoyages(Command):

    @property
    def name(self):
        return "generate-confidence-voyages"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--source", type=str, required=True, help=HELP_SOURCE),
            Option(
                "--min-confidence",
                type=str,
                required=True,
                choices=["2", "3", "4"],
                help=HELP_MIN_CONFIDENCE,
            ),
            Option("--output", type=str, required=True, help=HELP_OUTPUT),
            Option("--project", type=str, required=True, help=HELP_PROJECT),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
