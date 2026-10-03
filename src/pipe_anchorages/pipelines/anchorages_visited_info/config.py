from __future__ import annotations
from dataclasses import dataclass, field

from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class AnchoragesVisitedInfoConfig(PipelineConfig):
    bq_input_loitering: str
    bq_input_encounters: str
    bq_input_ais_gaps: str
    bq_input_named_anchorages: str
    bq_output: str
    labels: dict = field(default_factory=dict)
    project: str = None
    dry_run: bool = False

    # date_range it is declare in the base class as positional/required.
    # TODO: make it optional.
    date_range: tuple[str, str] = None
