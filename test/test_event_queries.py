from pipe_anchorages.anchorages_pipeline import create_queries


class DummyOptions(object):
    def __init__(
        self, start_date, end_date, messages_table="SOURCE_TABLE", segments_table="SEGMENTS_TABLE_"
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.messages_table = messages_table
        self.segments_table = segments_table


def test_create_queries_1():
    # args = DummyOptions("2016-01-01", "2016-01-01")
    # assert list(create_queries(args, date(2016, 1, 1), date(2016, 1, 1))) == ["""
    # SELECT seg_id AS ident, ssvid, lat, lon, speed,
    #         CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp
    # FROM `SOURCE_TABLE`
    # WHERE date(timestamp) BETWEEN '2016-01-01' AND '2016-01-01'

    # """]

    args = DummyOptions(
        start_date="2012-05-01",
        end_date="2017-05-15",
    )
    assert create_queries(args) == [
        """
        WITH

        destinations AS (
          SELECT seg_id, _TABLE_SUFFIX AS table_suffix,
              CASE
                WHEN ARRAY_LENGTH(cumulative_destinations) = 0 THEN NULL
                ELSE (SELECT MAX(destination)
                      OVER (ORDER BY count DESC)
                      FROM UNNEST(cumulative_destinations)
                      LIMIT 1)
                END AS destination
          FROM `SEGMENTS_TABLE_*`
          WHERE _TABLE_SUFFIX BETWEEN '20120501' AND '20150125'
        ),

        message_with_timestamps_in_seconds as (
           select cast(UNIX_MILLIS(timestamp) as FLOAT64) / 1000  AS timestamp,
                (60 * 1) as thin,
                format_date("%Y%m%d", date(timestamp)) as table_suffix,
                 * except (timestamp)
           from`SOURCE_TABLE*`
           WHERE date(timestamp) BETWEEN '2012-05-01' AND '2015-01-25'
             AND seg_id IS NOT NULL
             AND lat IS NOT NULL
             AND lon IS NOT NULL
             AND speed IS NOT NULL
             ),

        position_messages AS (
            SELECT ssvid, seg_id, lat, lon, timestamp, speed,  table_suffix
              FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ssvid, cast(floor(timestamp / thin) AS INT64)
                        ORDER BY ABS(timestamp / thin - FLOOR(timestamp / thin) - 0.5) ASC,
                        timestamp, ssvid, lat, lon, speed, course
                        ) ndx,
                from message_with_timestamps_in_seconds
            )
            WHERE ndx = 1
        )

    SELECT ssvid as ident,
           lat,
           lon,
           timestamp,
           destination,
           speed
    FROM position_messages
    JOIN destinations
    USING (seg_id, table_suffix)
    """, """
        WITH

        destinations AS (
          SELECT seg_id, _TABLE_SUFFIX AS table_suffix,
              CASE
                WHEN ARRAY_LENGTH(cumulative_destinations) = 0 THEN NULL
                ELSE (SELECT MAX(destination)
                      OVER (ORDER BY count DESC)
                      FROM UNNEST(cumulative_destinations)
                      LIMIT 1)
                END AS destination
          FROM `SEGMENTS_TABLE_*`
          WHERE _TABLE_SUFFIX BETWEEN '20150126' AND '20170515'
        ),

        message_with_timestamps_in_seconds as (
           select cast(UNIX_MILLIS(timestamp) as FLOAT64) / 1000  AS timestamp,
                (60 * 1) as thin,
                format_date("%Y%m%d", date(timestamp)) as table_suffix,
                 * except (timestamp)
           from`SOURCE_TABLE*`
           WHERE date(timestamp) BETWEEN '2015-01-26' AND '2017-05-15'
             AND seg_id IS NOT NULL
             AND lat IS NOT NULL
             AND lon IS NOT NULL
             AND speed IS NOT NULL
             ),

        position_messages AS (
            SELECT ssvid, seg_id, lat, lon, timestamp, speed,  table_suffix
              FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ssvid, cast(floor(timestamp / thin) AS INT64)
                        ORDER BY ABS(timestamp / thin - FLOOR(timestamp / thin) - 0.5) ASC,
                        timestamp, ssvid, lat, lon, speed, course
                        ) ndx,
                from message_with_timestamps_in_seconds
            )
            WHERE ndx = 1
        )

    SELECT ssvid as ident,
           lat,
           lon,
           timestamp,
           destination,
           speed
    FROM position_messages
    JOIN destinations
    USING (seg_id, table_suffix)
    """
    ]
