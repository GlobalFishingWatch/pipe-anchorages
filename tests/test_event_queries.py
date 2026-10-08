from pipe_anchorages.queries.anchorage_points import AnchoragePointsQuery


class DummyOptions(object):
    def __init__(
        self, start_date, end_date, bq_in_messages="SOURCE_TABLE", bq_in_segments="SEGMENTS_TABLE_"
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.bq_in_messages = bq_in_messages
        self.bq_in_segments = bq_in_segments


def test_query_renders_single_table():
    query = AnchoragePointsQuery(
        source_messages="SOURCE_TABLE",
        start_date="2016-01-01",
        end_date="2016-01-01",
    )
    expected = """
SELECT
    ssvid AS ident,
    lat,
    lon,
    CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp,
    destination,
    speed
FROM
    `SOURCE_TABLE`
WHERE
    date(timestamp) BETWEEN '2016-01-01' AND '2016-01-01'
    AND seg_id IS NOT NULL
    AND lat IS NOT NULL
    AND lon IS NOT NULL
    AND speed IS NOT NULL
"""
    assert query.render() == expected.strip("\n")
