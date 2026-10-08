import math
from collections import namedtuple

import apache_beam as beam

from pipe_anchorages import common as cmn
from pipe_anchorages.distance import distance
from pipe_anchorages.find_anchorage_points import AnchoragePoint

StationaryPeriod = namedtuple(
    "StationaryPeriod", ["location", "start_time", "duration", "rms_drift_radius", "destination"]
)

ActiveAndStationary = namedtuple("ActiveAndStationary", ["active_records", "stationary_periods"])


class GroupStationaryPeriodsByS2Cell(beam.PTransform):

    def __init__(self, min_duration, max_distance, min_unique_vessels, fishing_vessel_list):
        self.min_duration = min_duration
        self.max_distance = max_distance
        self.min_unique_vessels = min_unique_vessels
        self.fishing_vessel_list = fishing_vessel_list
        self.fishing_vessel_set = None

    def split_on_movement(self, item):
        # extract long stationary periods from the record. Stationary periods are returned
        # separately: anything over the threshold time will be reduced to just the start
        # and end points of the period. The remaining points will summarized and returned
        # as a stationary period
        ssvid, records = item

        active_records = []
        stationary_periods = []
        current_period = []

        for rcd in records:
            if current_period:
                first_rcd = current_period[0]
                if distance(rcd.location, first_rcd.location) > self.max_distance:
                    if current_period[-1].timestamp - first_rcd.timestamp > self.min_duration:
                        active_records.append(first_rcd)
                        if current_period[-1] != first_rcd:
                            active_records.append(current_period[-1])
                        num_points = len(current_period)
                        duration = current_period[-1].timestamp - first_rcd.timestamp
                        mean_lat = sum(x.location.lat for x in current_period) / num_points
                        mean_lon = sum(x.location.lon for x in current_period) / num_points
                        mean_location = cmn.LatLon(mean_lat, mean_lon)
                        rms_drift_radius = math.sqrt(
                            sum(distance(x.location, mean_location) ** 2 for x in current_period)
                            / num_points
                        )
                        stationary_periods.append(
                            StationaryPeriod(
                                mean_location,
                                first_rcd.timestamp,
                                duration,
                                rms_drift_radius,
                                first_rcd.destination,
                            )
                        )
                    else:
                        active_records.extend(current_period)
                    current_period = []
            current_period.append(rcd)
        active_records.extend(current_period)

        return (
            ssvid,
            ActiveAndStationary(
                active_records=active_records, stationary_periods=stationary_periods
            ),
        )

    def extract_stationary(self, item):
        ssvid, combined = item
        return [
            (sp.location.S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token(), (ssvid, sp))
            for sp in combined.stationary_periods
        ]

    def extract_active(self, item):
        ssvid, combined = item
        return [
            (ar.location.S2CellId(cmn.ANCHORAGES_S2_SCALE).to_token(), (ssvid, ar))
            for ar in combined.active_records
        ]

    def create_anchorage_pts(self, item, fishing_vessel_list):
        if self.fishing_vessel_set is None:
            self.fishing_vessel_set = set(fishing_vessel_list)
        value = AnchoragePoint.from_cell_visits(item, self.fishing_vessel_set)
        return [] if (value is None) else [value]

    def has_enough_vessels(self, item):
        return len(item.vessels) >= self.min_unique_vessels

    def expand(self, ais_source):
        combined = ais_source | beam.Map(self.split_on_movement)
        stationary = combined | beam.FlatMap(self.extract_stationary)
        active = combined | beam.FlatMap(self.extract_active)
        return (
            (stationary, active)
            | beam.CoGroupByKey()
            | beam.FlatMap(self.create_anchorage_pts, self.fishing_vessel_list)
            | beam.Filter(self.has_enough_vessels)
        )
