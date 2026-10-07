from __future__ import annotations
from dataclasses import dataclass

from gfw.common.config import PipelineConfig


@dataclass(frozen=True, kw_only=True)
class AnchoragePointsConfig(PipelineConfig):
    bq_in_messages: str
    bq_in_segments: str
    bq_out_anchorage_points: str
    gcs_in_fishing_ssvids: str

    # Defaults must be declared (they shadow PipelineConfig's own start_date/end_date
    # properties, which read from date_range) -- without them the inherited property
    # wins and raises, since this command takes --start-date/--end-date instead of
    # --date-range like pipe-gaps' commands do.
    # TODO: switch the CLI to a single --date-range option and drop these fields.
    start_date: str = None
    end_date: str = None

    min_positions: int = 200
    min_unique_vessels: int = 20
    stationary_period_min_duration_minutes: int = 720
    stationary_period_max_distance_km: float = 0.5

    # date_range is declared in the base class as required; see start_date above.
    date_range: tuple[str, str] = None
