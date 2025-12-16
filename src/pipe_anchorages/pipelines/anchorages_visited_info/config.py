from __future__ import annotations
from dataclasses import dataclass, field

# This command does not use beam but PipelineConfig has generic functionality.
# TODO: move PipelineConfig to a more generic package inside gfw-common lib.
from gfw.common.beam.pipeline.config import PipelineConfig


# TODO: Move kw_only=True to base class for consistency.
# TODO: This allows to declare positional arguments in the subclass.
@dataclass(kw_only=True)
class AnchoragesVisitedInfoConfig(PipelineConfig):
    bq_input_loitering: str
    bq_input_encounters: str
    bq_input_ais_gaps: str
    bq_input_named_anchorages: str
    bq_output: str
    bq_staging_suffix: str = "_staging"
    gcs_bucket: str
    gcs_prefix: str = "tmp"
    labels: dict = field(default_factory=dict)
    project: str = None
    dry_run: bool = False

    # date_range it is declare in the base class as positional/required.
    # TODO: make it optional.
    date_range: tuple[str, str] = None
