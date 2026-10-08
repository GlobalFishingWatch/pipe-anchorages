import datetime
import functools
from types import SimpleNamespace

import apache_beam as beam
import pytest
from apache_beam.testing.util import assert_that

from gfw.common.beam.transforms.bigquery import FakeReadFromBigQuery

from pipe_anchorages.assets import schemas
from pipe_anchorages.pipelines.anchorage_locations.main import AnchorageLocationsQuery, run

# The in-process runner: DirectRunner would pick Prism, which runs as a subprocess and stages an
# sdist of the package in the working directory.
RUNNER = "FnApiRunner"

T0 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc).timestamp()
STAY_POSITIONS = 40
EVERY_MINUTES = 10
STAY_HOURS = (STAY_POSITIONS - 1) * EVERY_MINUTES / 60


def stay_then_leave(ssvid, lat, lon):
    """Positions of a vessel stopped at (lat, lon) for STAY_HOURS, then moving ~5 km away.

    A stationary period is only closed (and counted) once the vessel moves away.
    """
    stay = [
        dict(ident=ssvid, lat=lat, lon=lon, timestamp=T0 + i * EVERY_MINUTES * 60,
             destination=None, speed=0.0)
        for i in range(STAY_POSITIONS)
    ]
    leave = dict(ident=ssvid, lat=lat + 0.05, lon=lon,
                 timestamp=T0 + STAY_POSITIONS * EVERY_MINUTES * 60, destination=None, speed=10.0)
    return stay + [leave]


TWO_VESSELS_AT_ONE_ANCHORAGE = (
    stay_then_leave("111", -34.6, -58.4) + stay_then_leave("222", -34.6001, -58.4001)
)


class AssertWritten(beam.PTransform):
    """Stands in for WriteToBigQuery: checks the rows to be written, inside the pipeline."""

    def __init__(self, check, **kwargs):
        super().__init__()
        self._check = check

    def expand(self, pcoll):
        assert_that(pcoll, self._check)
        return pcoll


@pytest.fixture
def fishing_ssvids(tmp_path):
    def write(*ssvids):
        path = tmp_path / "fishing_mmsi.txt"
        path.write_text("".join(f"{s}\n" for s in ssvids) or "999\n")
        return str(path)

    return write


def run_pipeline(rows, check, fishing_ssvids_path, reads=None, writes=None, **overrides):
    """Runs the pipeline on `rows` (shaped like the query's output) and checks what it writes."""

    def read_factory(**kwargs):
        if reads is not None:
            reads.append(kwargs)
        return FakeReadFromBigQuery(elements=rows, **kwargs)

    def write_factory(**kwargs):
        if writes is not None:
            writes.append(kwargs)
        return AssertWritten(check, **kwargs)

    config = dict(
        bq_in_messages="project.dataset.messages_positions",
        bq_out_anchorage_locations="project.dataset.anchorage_locations",
        start_date="2024-01-01",
        end_date="2024-01-07",
        gcs_in_fishing_ssvids=fishing_ssvids_path,
        min_positions=10,
        min_unique_vessels=2,
        stationary_period_min_duration_minutes=60,
        stationary_period_max_distance_km=0.5,
        labels={"environment": "development", "stage": "anchorages"},
        unknown_unparsed_args=[],
        unknown_parsed_args={"project": "test-project"},
    )
    config.update(overrides)

    return run(
        SimpleNamespace(**config),
        read_from_bigquery_factory=read_factory,
        write_to_bigquery_factory=write_factory,
        runner=RUNNER,
    )


def check_one_anchorage(rows, fishing_vessels):
    assert len(rows) == 1, rows
    (row,) = rows
    assert row["total_visits"] == 2
    assert row["unique_stationary_ssvid"] == 2
    assert row["unique_total_ssvid"] == 2
    assert row["stationary_ssvid_days"] == pytest.approx(2 * STAY_HOURS / 24)
    assert row["unique_stationary_fishing_ssvid"] == fishing_vessels
    assert row["stationary_fishing_ssvid_days"] == pytest.approx(fishing_vessels * STAY_HOURS / 24)
    assert row["lat"] == pytest.approx(-34.60005)
    assert row["lon"] == pytest.approx(-58.40005)
    assert set(row) == {f["name"] for f in schemas.get_schema("anchorage_locations.json")}


def check_no_rows(rows):
    assert rows == []


def test_run_finds_the_anchorage_where_vessels_stay(fishing_ssvids):
    check = functools.partial(check_one_anchorage, fishing_vessels=0)

    assert run_pipeline(TWO_VESSELS_AT_ONE_ANCHORAGE, check, fishing_ssvids()) == 0


def test_run_counts_fishing_vessels_from_the_side_input(fishing_ssvids):
    check = functools.partial(check_one_anchorage, fishing_vessels=1)

    assert run_pipeline(TWO_VESSELS_AT_ONE_ANCHORAGE, check, fishing_ssvids("111")) == 0


def test_run_skips_anchorages_with_too_few_vessels(fishing_ssvids):
    assert run_pipeline(
        TWO_VESSELS_AT_ONE_ANCHORAGE, check_no_rows, fishing_ssvids(), min_unique_vessels=3
    ) == 0


def test_run_reads_the_date_range_and_writes_the_output_table(fishing_ssvids):
    reads, writes = [], []

    run_pipeline([], check_no_rows, fishing_ssvids(), reads=reads, writes=writes)

    (read,) = reads
    assert "project.dataset.messages_positions" in read["query"]
    assert "date(timestamp) >= '2024-01-01'" in read["query"]
    assert "date(timestamp) < '2024-01-07'" in read["query"]
    (write,) = writes
    assert write["table"] == "project.dataset.anchorage_locations"
    assert [f["name"] for f in write["schema"]["fields"]] == [
        f["name"] for f in schemas.get_schema("anchorage_locations.json")
    ]


def test_query_renders_single_table():
    query = AnchorageLocationsQuery(
        source_messages="SOURCE_TABLE",
        start_date="2016-01-01",
        end_date="2016-01-02",
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
    date(timestamp) >= '2016-01-01'
    AND date(timestamp) < '2016-01-02'
    AND seg_id IS NOT NULL
    AND lat IS NOT NULL
    AND lon IS NOT NULL
    AND speed IS NOT NULL
"""
    assert query.render() == expected.strip("\n")
