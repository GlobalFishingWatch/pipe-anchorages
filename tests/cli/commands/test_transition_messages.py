import argparse

from types import SimpleNamespace

import pytest

from apache_beam.options.pipeline_options import GoogleCloudOptions
from gfw.common.beam.pipeline.base import Pipeline

from pipe_anchorages import thin_port_messages_pipeline
from pipe_anchorages.cli import main
from pipe_anchorages.cli.commands.transition_messages import TransitionMessages


BASE_ARGS = [
    "transition-messages",
    "--bq-in-named-anchorages", "project.dataset.anchorages",
    "--bq-in-messages", "project.dataset.messages",
    "--bq-out-transition-messages", "project.dataset.output",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.transition_messages.thin_port_messages_pipeline.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.bq_in_named_anchorages == "project.dataset.anchorages"
    assert config.bq_in_messages == "project.dataset.messages"
    assert config.bq_out_transition_messages == "project.dataset.output"
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
        "pipe_anchorages.cli.commands.transition_messages.thin_port_messages_pipeline.run",
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
    flags = {opt.flags[0]: opt for opt in TransitionMessages.transition_options()}

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


def test_gfw_pipeline_resolves_project_and_labels_from_config():
    # thin_port_messages_pipeline.run() builds a Pipeline, and translates
    # config.labels (a dict, from the CLI's shared --labels option) into the
    # list-of-"key=value" strings GoogleCloudOptions.labels actually expects --
    # this exercises that translation directly, without needing a full pipeline run.
    #
    # Uses explicit kwargs (project=/runner=) rather than unparsed_args=, which is
    # what run() itself passes through in production for real Beam/Dataflow flags:
    # unparsed_args triggers apache_beam's own PipelineOptions(...), which does a
    # *global* scan of every currently-imported PipelineOptions subclass -- safe in
    # a real single-command process, but collides here with this same test *session*
    # also importing other not-yet-migrated pipelines' *Options(PipelineOptions)
    # classes (name_anchorage_options.py and friends), which still redeclare
    # overlapping flag names like --output_table. Not a production risk: each CLI
    # invocation is its own process, and those sibling classes are only ever lazily
    # imported when their own (still legacy) commands are actually invoked.
    labels = {"team": "pipeline", "env": "prod"}

    gfw_pipeline = Pipeline(
        project="test-project",
        runner="DirectRunner",
        labels=[f"{key}={value}" for key, value in labels.items()],
    )

    cloud_options = gfw_pipeline.cloud_options
    assert isinstance(cloud_options, GoogleCloudOptions)
    assert cloud_options.project == "test-project"
    assert sorted(cloud_options.labels) == ["env=prod", "team=pipeline"]


def test_run_forwards_config_file_beam_options_to_pipeline(mocker):
    # Any Apache Beam/Dataflow option can be set from the --config-file YAML, not
    # just the CLI flags -- the CLI framework routes config-file keys that aren't
    # one of this command's own Options into config.unknown_parsed_args, and run()
    # must forward that dict into Pipeline(**options), the same way pipe-gaps'
    # PipelineFactory does with its own `**self._config.unknown_parsed_args`.
    #
    # Only patches Pipeline itself, not the Beam transforms downstream -- those
    # build a real DAG against a mocked Pipeline and fail past the point this test
    # cares about (e.g. apache_beam's own pickling of a DoFn against a Mock, or a
    # bare PipelineOptions() call that picks up every currently-imported
    # PipelineOptions subclass -- including sibling, not-yet-migrated ones like
    # NameAnchorageOptions -- and fails parsing pytest's own argv against their
    # union; SystemExit, not Exception, so it needs its own except clause below),
    # which is expected and ignored here.
    mock_pipeline_cls = mocker.patch("pipe_anchorages.thin_port_messages_pipeline.Pipeline")

    config = SimpleNamespace(
        bq_in_messages="project.dataset.messages",
        bq_in_named_anchorages="project.dataset.anchorages",
        bq_out_transition_messages="project.dataset.output",
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

    try:
        thin_port_messages_pipeline.run(config)
    except (Exception, SystemExit):
        pass

    mock_pipeline_cls.assert_called_once_with(
        unparsed_args=[],
        labels=["team=pipeline"],
        project="test-project",
        max_num_workers=50,
    )
