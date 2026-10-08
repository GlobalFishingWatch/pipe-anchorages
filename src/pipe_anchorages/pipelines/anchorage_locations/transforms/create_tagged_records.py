import datetime

import apache_beam as beam

from pipe_anchorages.records import VesselInfoRecord, VesselLocationRecord


class CreateTaggedRecords(beam.PTransform):
    def __init__(self, min_required_positions, thin=True):
        self.min_required_positions = min_required_positions
        self.thin = thin
        self.FIVE_MINUTES = datetime.timedelta(minutes=5)

    def order_by_timestamp(self, item):
        ident, records = item
        records = sorted(records, key=lambda x: (x.timestamp, x.speed, x.location))
        return ident, records

    def dedup_by_timestamp(self, item):
        key, source = item
        seen = set()
        sink = []
        for x in sorted(source, key=lambda x: (x.timestamp, x.speed, x.location)):
            if x.timestamp not in seen:
                sink.append(x)
                seen.add(x.timestamp)
        return (key, sink)

    def long_enough(self, item):
        ident, records = item
        return len(records) >= self.min_required_positions

    def thin_records(self, item):
        if not self.thin:
            return item
        ident, records = item
        last_timestamp = datetime.datetime(datetime.MINYEAR, 1, 1, tzinfo=datetime.timezone.utc)
        thinned = []
        for rcd in records:
            if (rcd.timestamp - last_timestamp) >= self.FIVE_MINUTES:
                last_timestamp = rcd.timestamp
                thinned.append(rcd)
        return ident, thinned

    def tag_records(self, item):
        ident, records = item
        dest = ""
        tagged = []
        for rcd in records:
            if isinstance(rcd, VesselInfoRecord):
                dest = rcd.destination
            elif isinstance(rcd, VesselLocationRecord):
                tagged.append(rcd._replace(destination=dest))
            else:
                raise RuntimeError("unknown type {}".format(type(rcd)))
        return (ident, tagged)

    def expand(self, vessel_records):
        return (
            vessel_records
            | beam.GroupByKey()
            | beam.Map(self.order_by_timestamp)
            | beam.Map(self.dedup_by_timestamp)
            | beam.Filter(self.long_enough)
            | beam.Map(self.tag_records)
            | beam.Map(self.thin_records)
        )
