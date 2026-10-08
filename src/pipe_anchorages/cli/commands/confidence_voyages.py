from typing import Any
from types import SimpleNamespace

from gfw.common.cli import Command, Option

from pipe_anchorages.confidence_voyages import run


DESCRIPTION = """\
Generates the confidence voyages table for a given minimum confidence level.

A "voyage" is the combination of a vessel's previous port_visit's end and next
port_visit's start.
"""

HELP_BQ_IN_PORT_VISITS = "BigQuery table with port visits (Format str, ex: dataset.table)."
HELP_MIN_CONFIDENCE = "The minimal confidence to detect the voyages (Format str, ex: 3)."
HELP_BQ_OUT_VOYAGES = "BigQuery table in which to store the voyages (ex: project.dataset.table)."
HELP_PROJECT = "The GCP project billed for the processing of this step."


class ConfidenceVoyages(Command):

    @property
    def name(self):
        return "confidence-voyages"

    @property
    def description(self):
        return DESCRIPTION

    @property
    def options(self):
        return [
            Option("--bq-in-port-visits", type=str, required=True, help=HELP_BQ_IN_PORT_VISITS),
            Option("--bq-out-voyages", type=str, required=True, help=HELP_BQ_OUT_VOYAGES),
            Option("--project", type=str, required=True, help=HELP_PROJECT),
            Option(
                "--min-confidence",
                type=str,
                required=True,
                choices=["2", "3", "4"],
                help=HELP_MIN_CONFIDENCE,
            ),
        ]

    @classmethod
    def run(cls, config: SimpleNamespace, **kwargs: Any) -> Any:
        return run(config, **kwargs)
