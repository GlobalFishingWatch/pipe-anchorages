import argparse

from types import SimpleNamespace

import pytest

from pipe_anchorages import port_visits_pipeline
from pipe_anchorages.cli import main


BASE_ARGS = [
    "port-visits",
    "--bq-in-port-events", "project.dataset.port_events",
    "--bq-in-segment-info", "project.dataset.segment_info",
    "--bq-out-port-visits", "project.dataset.port_visits",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.port_visits.port_visits_pipeline.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.bq_in_port_events == "project.dataset.port_events"
    assert config.bq_in_segment_info == "project.dataset.segment_info"
    assert config.bq_out_port_visits == "project.dataset.port_visits"
    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-01-07"
    assert config.bad_segs is None
    assert config.max_inter_seg_dist_nm == 60.0
    assert config.wait_for_job is False


def test_cli_requires_port_events_table(mocker):
    mocker.patch(
        "pipe_anchorages.cli.commands.port_visits.port_visits_pipeline.run",
        return_value=0,
    )
    args = [
        a for a in BASE_ARGS
        if a not in ("--bq-in-port-events", "project.dataset.port_events")
    ]

    with pytest.raises(argparse.ArgumentTypeError, match="bq_in_port_events"):
        main.run(args)


def test_run_forwards_config_file_beam_options_to_pipeline(mocker):
    # Same forwarding contract as thin_port_messages_pipeline.run(): config-file keys
    # that aren't one of this command's own Options land in config.unknown_parsed_args,
    # and run() must forward that dict into Pipeline(**options), matching pipe-gaps'
    # PipelineFactory (`**self._config.unknown_parsed_args`).
    #
    # Only patches Pipeline itself, not the Beam transforms downstream -- those build
    # a real DAG against a mocked Pipeline and fail past the point this test cares
    # about (e.g. apache_beam's own pickling of a DoFn against a Mock), which is
    # expected and ignored here.
    mock_pipeline_cls = mocker.patch("pipe_anchorages.port_visits_pipeline.Pipeline")

    config = SimpleNamespace(
        bq_in_port_events="project.dataset.port_events",
        bq_in_segment_info="project.dataset.segment_info",
        bq_out_port_visits="project.dataset.port_visits",
        start_date="2024-01-01",
        end_date="2024-01-07",
        config="unused.yaml",
        bad_segs=None,
        max_inter_seg_dist_nm=60.0,
        wait_for_job=False,
        labels={"team": "pipeline"},
        unknown_unparsed_args=[],
        unknown_parsed_args={"project": "test-project", "max_num_workers": 50},
    )

    try:
        port_visits_pipeline.run(config)
    except Exception:
        pass

    mock_pipeline_cls.assert_called_once_with(
        unparsed_args=[],
        labels=["team=pipeline"],
        project="test-project",
        max_num_workers=50,
    )
