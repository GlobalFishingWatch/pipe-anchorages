import argparse

from types import SimpleNamespace

import pytest

from pipe_anchorages import anchorages_pipeline
from pipe_anchorages.cli import main


BASE_ARGS = [
    "anchorage-points",
    "--bq-in-messages", "project.dataset.messages",
    "--bq-in-segments", "project.dataset.segments",
    "--bq-out-anchorage-points", "project.dataset.anchorage_points",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
    "--gcs-in-fishing-ssvids", "gs://bucket/fishing_mmsi.txt",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.anchorage_points.anchorages_pipeline.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.bq_in_messages == "project.dataset.messages"
    assert config.bq_in_segments == "project.dataset.segments"
    assert config.bq_out_anchorage_points == "project.dataset.anchorage_points"
    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-01-07"
    assert config.gcs_in_fishing_ssvids == "gs://bucket/fishing_mmsi.txt"
    assert config.min_positions == 200
    assert config.stationary_period_min_duration_minutes == 720
    assert config.stationary_period_max_distance_km == 0.5
    assert config.min_unique_vessels == 20


def test_cli_requires_gcs_in_fishing_ssvids(mocker):
    mocker.patch(
        "pipe_anchorages.cli.commands.anchorage_points.anchorages_pipeline.run",
        return_value=0,
    )
    excluded = ("--gcs-in-fishing-ssvids", "gs://bucket/fishing_mmsi.txt")
    args = [a for a in BASE_ARGS if a not in excluded]

    with pytest.raises(argparse.ArgumentTypeError, match="gcs_in_fishing_ssvids"):
        main.run(args)


def test_run_forwards_config_file_beam_options_to_pipeline(mocker):
    # Same forwarding contract as thin_port_messages_pipeline.run()/port_visits_pipeline.run():
    # config-file keys that aren't one of this command's own Options land in
    # config.unknown_parsed_args, and run() must forward that dict into
    # Pipeline(**options), matching pipe-gaps' PipelineFactory.
    #
    # Only patches Pipeline itself, not the Beam transforms downstream -- those build
    # a real DAG against a mocked Pipeline and fail past the point this test cares
    # about (e.g. apache_beam's own pickling of a DoFn against a Mock, or a bare
    # PipelineOptions() call that picks up every currently-imported PipelineOptions
    # subclass -- including sibling, not-yet-migrated ones like NameAnchorageOptions
    # -- and fails parsing pytest's own argv against their union; SystemExit, not
    # Exception, so it needs its own except clause below), which is expected and
    # ignored here.
    mock_pipeline_cls = mocker.patch("pipe_anchorages.anchorages_pipeline.Pipeline")

    config = SimpleNamespace(
        bq_in_messages="project.dataset.messages",
        bq_in_segments="project.dataset.segments",
        bq_out_anchorage_points="project.dataset.anchorage_points",
        start_date="2024-01-01",
        end_date="2024-01-07",
        gcs_in_fishing_ssvids="gs://bucket/fishing_mmsi.txt",
        min_positions=200,
        stationary_period_min_duration_minutes=720,
        stationary_period_max_distance_km=0.5,
        min_unique_vessels=20,
        labels={"team": "pipeline"},
        unknown_unparsed_args=[],
        unknown_parsed_args={"project": "test-project", "max_num_workers": 50},
    )

    try:
        anchorages_pipeline.run(config)
    except (Exception, SystemExit):
        pass

    mock_pipeline_cls.assert_called_once_with(
        unparsed_args=[],
        labels=["team=pipeline"],
        project="test-project",
        max_num_workers=50,
    )
