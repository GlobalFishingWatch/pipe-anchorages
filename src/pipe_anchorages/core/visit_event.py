from collections import namedtuple

VisitEvent = namedtuple(
    "VisitEvent",
    [
        "anchorage_id",
        "lat",
        "lon",
        "vessel_lat",
        "vessel_lon",
        "ssvid",
        "seg_id",
        "vessel_id",
        "timestamp",
        "event_type",
        "last_timestamp",
    ],
)
