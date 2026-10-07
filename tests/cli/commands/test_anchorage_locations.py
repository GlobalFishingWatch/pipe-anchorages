import argparse

from types import SimpleNamespace

import pytest

from apache_beam.runners import PipelineState

from pipe_anchorages.pipelines.anchorage_points import main as anchorage_points
from pipe_anchorages.pipelines.anchorage_points.main import AnchoragePointsSink
from pipe_anchorages.cli import main


BASE_ARGS = [
    "anchorage-locations",
    "--bq-in-messages", "project.dataset.messages",
    "--bq-in-segments", "project.dataset.segments",
    "--bq-out-anchorage-locations", "project.dataset.anchorage_locations",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
    "--gcs-in-fishing-ssvids", "gs://bucket/fishing_mmsi.txt",
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.anchorage_locations.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.bq_in_messages == "project.dataset.messages"
    assert config.bq_in_segments == "project.dataset.segments"
    assert config.bq_out_anchorage_locations == "project.dataset.anchorage_locations"
    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-01-07"
    assert config.gcs_in_fishing_ssvids == "gs://bucket/fishing_mmsi.txt"
    assert config.min_positions == 200
    assert config.stationary_period_min_duration_minutes == 720
    assert config.stationary_period_max_distance_km == 0.5
    assert config.min_unique_vessels == 20


def test_cli_executes_run_with_mock_bq_clients(tmp_path):
    # End-to-end: the pipeline really constructs its Beam DAG and runs it
    # through the DirectRunner, but with BigQuery sources/sinks swapped for
    # the gfw-common fakes, and the fishing-vessels file pointed at /tmp.
    fishing_ssvids = tmp_path / "fishing_mmsi.txt"
    fishing_ssvids.write_text("416000001\n")

    args = []
    for a in BASE_ARGS:
        if a == "gs://bucket/fishing_mmsi.txt":
            args.append(str(fishing_ssvids))
        else:
            args.append(a)
    args.append("--mock-bq-clients")
    args.extend(["--project", "test-project"])

    result = main.run(args)
    exit_code = result[0] if isinstance(result, tuple) else result
    assert exit_code == 0


def test_cli_requires_gcs_in_fishing_ssvids(mocker):
    mocker.patch(
        "pipe_anchorages.cli.commands.anchorage_locations.run",
        return_value=0,
    )
    excluded = ("--gcs-in-fishing-ssvids", "gs://bucket/fishing_mmsi.txt")
    args = [a for a in BASE_ARGS if a not in excluded]

    with pytest.raises(argparse.ArgumentTypeError, match="gcs_in_fishing_ssvids"):
        main.run(args)


def test_run_forwards_config_file_beam_options_to_pipeline(capture_pipeline_init, mocker):
    # Same forwarding contract as thin_port_messages_pipeline.run()/port_visits_pipeline.run():
    # config-file keys that aren't one of this command's own Options land in
    # config.unknown_parsed_args, and run() must forward that dict into
    # Pipeline(**options), matching pipe-gaps' PipelineFactory.
    config = SimpleNamespace(
        bq_in_messages="project.dataset.messages",
        bq_in_segments="project.dataset.segments",
        bq_out_anchorage_locations="project.dataset.anchorage_locations",
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

    mock_pipeline_cls = capture_pipeline_init(
        "pipe_anchorages.pipelines.anchorage_points.main.Pipeline",
        anchorage_points.run,
        config,
    )

    mock_pipeline_cls.assert_called_once_with(
        name="pipe-anchorages",
        version=mocker.ANY,
        dag=mocker.ANY,
        unparsed_args=[],
        labels={"team": "pipeline"},
        project="test-project",
        max_num_workers=50,
    )


def test_run_builds_the_linear_dag_without_executing_it(mocker, tmp_path):
    # Constructs the whole DAG (sources, core chain, sink) against a throwaway
    # beam.Pipeline, but patches Pipeline.run so nothing executes -- there is no
    # BigQuery/GCS access in unit tests. Catches wiring errors in the assembly.
    # ReadFromText stats its path at construction, so use a local file for it.
    fishing_ssvids = tmp_path / "fishing_mmsi.txt"
    fishing_ssvids.write_text("416000001\n")

    config = SimpleNamespace(
        bq_in_messages="project.dataset.messages",
        bq_in_segments="project.dataset.segments",
        bq_out_anchorage_points="project.dataset.anchorage_points",
        start_date="2024-01-01",
        end_date="2024-01-07",
        gcs_in_fishing_ssvids=str(fishing_ssvids),
        min_positions=200,
        stationary_period_min_duration_minutes=720,
        stationary_period_max_distance_km=0.5,
        min_unique_vessels=20,
        labels={"team": "pipeline"},
        unknown_unparsed_args=[],
        unknown_parsed_args={"project": "test-project"},
    )

    captured = {}
    real_linear_dag = anchorage_points.LinearDag

    def capture_linear_dag(*args, **kwargs):
        captured.update(kwargs)
        return real_linear_dag(*args, **kwargs)

    mocker.patch.object(anchorage_points, "LinearDag", side_effect=capture_linear_dag)
    mocker.patch(
        "apache_beam.pipeline.Pipeline.run",
        return_value=mocker.Mock(state=PipelineState.RUNNING),
    )

    assert anchorage_points.run(config) == 0

    # 2024-01-01..2024-01-07 fits in a single 1000-day query chunk.
    assert len(captured["sources"]) == 1
    assert isinstance(captured["core"], anchorage_points.AnchoragePointsCore)
    assert len(captured["sinks"]) == 1
    assert isinstance(captured["sinks"][0], AnchoragePointsSink)
