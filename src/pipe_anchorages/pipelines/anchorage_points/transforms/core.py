import datetime
import logging

import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.find_anchorage_points import FindAnchoragePoints
from pipe_anchorages.records import VesselLocationRecord
from pipe_anchorages.pipelines.anchorage_points.config import AnchoragePointsConfig

logger = logging.getLogger(__name__)


def has_location_record(item):
    _, rcd = item
    return isinstance(rcd, VesselLocationRecord)


def encode_anchorage(anchorage) -> dict:
    return {
        "lat": anchorage.mean_location.lat,
        "lon": anchorage.mean_location.lon,
        "total_visits": anchorage.total_visits,
        "drift_radius": anchorage.rms_drift_radius,
        "top_destination": anchorage.top_destination,
        "unique_stationary_ssvid": len(anchorage.vessels),
        "unique_stationary_fishing_ssvid": len(anchorage.fishing_vessels),
        "unique_active_ssvid": anchorage.active_ssvids,
        "unique_total_ssvid": anchorage.total_ssvids,
        "active_ssvid_days": anchorage.active_ssvid_days,
        "stationary_ssvid_days": anchorage.stationary_ssvid_days,
        "stationary_fishing_ssvid_days": anchorage.stationary_fishing_ssvid_days,
        "s2id": anchorage.s2id,
    }


class AnchoragePointsCore(beam.PTransform):
    """Turns position messages into anchorage table records (row dicts).

    Groups the inline chain (CreateVesselRecords -> filter location records ->
    CreateTaggedRecords -> FindAnchoragePoints -> encode) into one composite
    PTransform, since LinearDag's core slot takes exactly one transform. The
    fishing vessel list is read here too, as a side input of
    FindAnchoragePoints -- LinearDag's own side_inputs slot would require
    implementing set_side_inputs, which we don't need yet.
    """

    def __init__(self, config: AnchoragePointsConfig) -> None:
        self.min_positions = config.min_positions
        self.min_duration = datetime.timedelta(
            minutes=config.stationary_period_min_duration_minutes
        )
        self.max_distance_km = config.stationary_period_max_distance_km
        self.min_unique_vessels = config.min_unique_vessels
        self.gcs_in_fishing_ssvids = config.gcs_in_fishing_ssvids

    def expand(self, xs):
        fishing_vessels = xs.pipeline | "ReadFishingVessels" >> beam.io.ReadFromText(
            self.gcs_in_fishing_ssvids
        )
        fishing_vessel_list = beam.pvalue.AsList(fishing_vessels)

        return (
            xs
            | cmn.CreateVesselRecords()
            | "FilterOutInfo" >> beam.Filter(has_location_record)
            | cmn.CreateTaggedRecords(self.min_positions)
            | FindAnchoragePoints(
                self.min_duration,
                self.max_distance_km,
                self.min_unique_vessels,
                fishing_vessel_list,
            )
            | beam.Map(encode_anchorage)
        )
