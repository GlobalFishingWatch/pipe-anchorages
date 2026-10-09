import argparse

import pytest

from gfw.common.config import PipelineConfigError
from pipe_anchorages.cli import main


BASE_ARGS = [
    "anchorage-locations",
    "--bq-in-messages", "project.dataset.messages",
    "--bq-out-anchorage-locations", "project.dataset.anchorage_locations",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
    "--gcs-in-fishing-ssvids", "gs://bucket/fishing_mmsi.txt",
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run_with_mock_bq_clients(tmp_path):
    # The pipeline really builds and runs its Beam DAG, with the BigQuery source/sink swapped for
    # gfw-common's fakes and the in-process FnApiRunner (DirectRunner would pick Prism, which
    # runs as a subprocess and stages an sdist of the package in the working directory).
    fishing_ssvids = tmp_path / "fishing_mmsi.txt"
    fishing_ssvids.write_text("416000001\n")
    args = list(BASE_ARGS)
    args[args.index("gs://bucket/fishing_mmsi.txt")] = str(fishing_ssvids)

    main.run(
        [*args, "--mock-bq-clients", "--project", "test-project", "--runner", "FnApiRunner"]
    )


@pytest.mark.parametrize(
    "flag",
    [
        "--bq-in-messages",
        "--bq-out-anchorage-locations",
        "--start-date",
        "--end-date",
        "--gcs-in-fishing-ssvids",
    ],
)
def test_cli_requires_argument(flag):
    args = list(BASE_ARGS)
    i = args.index(flag)
    del args[i:i + 2]

    with pytest.raises(argparse.ArgumentTypeError, match="Missing required arguments"):
        main.run(args)


def test_cli_rejects_an_invalid_date():
    args = list(BASE_ARGS)
    args[args.index("2024-01-07")] = "07/01/2024"

    with pytest.raises(SystemExit):
        main.run(args)


def test_cli_rejects_an_empty_date_range():
    args = list(BASE_ARGS)
    args[args.index("2024-01-07")] = "2024-01-01"

    with pytest.raises(PipelineConfigError, match=r"end_date .* must be after start_date"):
        main.run(args)
