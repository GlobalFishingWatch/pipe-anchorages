from __future__ import annotations
from dataclasses import dataclass

from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class ConfidenceVoyagesConfig(PipelineConfig):
    bq_in_port_visits: str
    bq_out_voyages: str
    min_confidence: str
    project: str = None

    # This pipeline always rebuilds the whole output table, with no date windowing, but
    # date_range it is declare in the base class as positional/required.
    # TODO: make it optional.
    date_range: tuple[str, str] = None
