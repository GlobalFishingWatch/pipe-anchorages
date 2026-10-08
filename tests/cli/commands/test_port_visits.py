import argparse
import logging

from types import SimpleNamespace

import pytest

from pipe_anchorages import port_visits_pipeline
from pipe_anchorages.cli import main


BASE_ARGS = [
    "port-visits",
    "--bq-in-port-state-transitions", "project.dataset.port_state_transitions",
    "--bq-in-segment-info", "project.dataset.segment_info",
    "--bq-out-port-visits", "project.dataset.port_visits",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
    "--labels", "team=test",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.port_visits.port_visits_pipeline.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.bq_in_port_state_transitions == "project.dataset.port_state_transitions"
    assert config.bq_in_segment_info == "project.dataset.segment_info"
    assert config.bq_out_port_visits == "project.dataset.port_visits"
    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-01-07"
    assert config.bad_segs is None
    assert config.max_inter_seg_dist_nm == 60.0
    assert config.wait_for_job is False
    assert config.anchorage_entry_dist_km == 3.0
    assert config.anchorage_exit_dist_km == 4.0
    assert config.stopping_speed_knots == 0.2
    assert config.starting_speed_knots == 0.5
    assert config.min_anchorage_gap_minutes == 240.0


def test_cli_requires_port_state_transitions_table(mocker):
    mocker.patch(
        "pipe_anchorages.cli.commands.port_visits.port_visits_pipeline.run",
        return_value=0,
    )
    excluded = ("--bq-in-port-state-transitions", "project.dataset.port_state_transitions")
    args = [a for a in BASE_ARGS if a not in excluded]

    with pytest.raises(argparse.ArgumentTypeError, match="bq_in_port_state_transitions"):
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
        bq_in_port_state_transitions="project.dataset.port_state_transitions",
        bq_in_segment_info="project.dataset.segment_info",
        bq_out_port_visits="project.dataset.port_visits",
        start_date="2024-01-01",
        end_date="2024-01-07",
        bad_segs=None,
        max_inter_seg_dist_nm=60.0,
        wait_for_job=False,
        anchorage_entry_dist_km=3.0,
        anchorage_exit_dist_km=4.0,
        stopping_speed_knots=0.2,
        starting_speed_knots=0.5,
        min_anchorage_gap_minutes=240.0,
        labels={"team": "pipeline"},
        unknown_unparsed_args=[],
        unknown_parsed_args={"project": "test-project", "max_num_workers": 50},
    )

    try:
        # Silences the real (expected, ignored) DAG's own noisy logging -- see comment above.
        logging.disable(logging.CRITICAL)
        port_visits_pipeline.run(config)
    except Exception:
        pass
    finally:
        logging.disable(logging.NOTSET)

    mock_pipeline_cls.assert_called_once_with(
        unparsed_args=[],
        labels={"team": "pipeline"},
        project="test-project",
        max_num_workers=50,
    )
