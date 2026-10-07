import argparse
import json

from types import SimpleNamespace

import pytest

from apache_beam.options.pipeline_options import GoogleCloudOptions
from gfw.common.beam.pipeline.base import Pipeline

from pipe_anchorages import thin_port_messages_pipeline
from pipe_anchorages.cli import main
from pipe_anchorages.cli.commands.port_state_transitions import PortStateTransitions


BASE_ARGS = [
    "port-state-transitions",
    "--bq-in-named-anchorages", "project.dataset.anchorages",
    "--bq-in-messages", "project.dataset.messages",
    "--bq-out-port-state-transitions", "project.dataset.output",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
    "--labels", "team=test",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.port_state_transitions.thin_port_messages_pipeline.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.bq_in_named_anchorages == "project.dataset.anchorages"
    assert config.bq_in_messages == "project.dataset.messages"
    assert config.bq_out_port_state_transitions == "project.dataset.output"
    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-01-07"
    assert config.wait_for_job is False
    assert config.anchorage_entry_dist_km == 3.0
    assert config.anchorage_exit_dist_km == 4.0
    assert config.stopping_speed_knots == 0.2
    assert config.starting_speed_knots == 0.5
    assert config.min_anchorage_gap_minutes == 240.0


def test_cli_requires_named_anchorages_table(mocker):
    mocker.patch(
        "pipe_anchorages.cli.commands.port_state_transitions.thin_port_messages_pipeline.run",
        return_value=0,
    )
    args = [
        a for a in BASE_ARGS
        if a not in ("--bq-in-named-anchorages", "project.dataset.anchorages")
    ]

    with pytest.raises(argparse.ArgumentTypeError, match="bq_in_named_anchorages"):
        main.run(args)


def test_transition_options_are_shared_with_port_visits():
    # port-visits imports and reuses this method rather than redeclaring the
    # same 5 Options, so the two commands can't drift apart. Locking in the
    # exact flag names/defaults here protects that shared contract.
    flags = {opt.flags[0]: opt for opt in PortStateTransitions.transition_options()}

    assert set(flags) == {
        "--anchorage-entry-dist-km",
        "--anchorage-exit-dist-km",
        "--stopping-speed-knots",
        "--starting-speed-knots",
        "--min-anchorage-gap-minutes",
    }
    assert flags["--anchorage-entry-dist-km"].default == 3.0
    assert flags["--anchorage-exit-dist-km"].default == 4.0
    assert flags["--stopping-speed-knots"].default == 0.2
    assert flags["--starting-speed-knots"].default == 0.5
    assert flags["--min-anchorage-gap-minutes"].default == 240.0


def test_pipeline_resolves_project_and_labels_from_config():
    # thin_port_messages_pipeline.run() builds a Pipeline, passing config.labels (a
    # dict, from the CLI's shared --labels option) straight through --
    # PipelineOptions.from_dictionary() JSON-encodes a dict value into a single
    # --labels=<json> flag, which GoogleCloudOptions understands natively (same
    # format as Beam's own documented --labels='{ "key": "value" }' usage). This
    # exercises that translation directly, without needing a full pipeline run.
    #
    # Uses explicit kwargs (project=/runner=) rather than unparsed_args=, which is
    # what run() itself passes through in production for real Beam/Dataflow flags:
    # unparsed_args triggers apache_beam's own PipelineOptions(...), which does a
    # *global* scan of every currently-imported PipelineOptions subclass. Explicit
    # kwargs keep this test isolated from that scan entirely.
    labels = {"team": "pipeline", "env": "prod"}

    pipeline = Pipeline(
        project="test-project",
        runner="DirectRunner",
        labels=labels,
    )

    cloud_options = pipeline.cloud_options
    assert isinstance(cloud_options, GoogleCloudOptions)
    assert cloud_options.project == "test-project"
    assert json.loads(cloud_options.labels[0]) == labels


def test_run_forwards_config_file_beam_options_to_pipeline(capture_pipeline_init):
    # Any Apache Beam/Dataflow option can be set from the --config-file YAML, not
    # just the CLI flags -- the CLI framework routes config-file keys that aren't
    # one of this command's own Options into config.unknown_parsed_args, and run()
    # must forward that dict into Pipeline(**options), the same way pipe-gaps'
    # PipelineFactory does with its own `**self._config.unknown_parsed_args`.
    config = SimpleNamespace(
        bq_in_messages="project.dataset.messages",
        bq_in_named_anchorages="project.dataset.anchorages",
        bq_out_port_state_transitions="project.dataset.output",
        start_date="2024-01-01",
        end_date="2024-01-07",
        ssvid_filter=None,
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

    mock_pipeline_cls = capture_pipeline_init(
        "pipe_anchorages.thin_port_messages_pipeline.Pipeline",
        thin_port_messages_pipeline.run,
        config,
    )

    mock_pipeline_cls.assert_called_once_with(
        unparsed_args=[],
        labels={"team": "pipeline"},
        project="test-project",
        max_num_workers=50,
    )
