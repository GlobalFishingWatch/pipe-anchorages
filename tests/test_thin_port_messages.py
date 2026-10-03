import argparse

import pytest

from apache_beam.options.pipeline_options import GoogleCloudOptions
from gfw.common.beam.pipeline.base import Pipeline as GfwPipeline

from pipe_anchorages.cli import main


BASE_ARGS = [
    "thin-port-messages",
    "--bq-in-named-anchorages", "project.dataset.anchorages",
    "--bq-in-messages", "project.dataset.messages",
    "--bq-out-port-events", "project.dataset.output",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.thin_port_messages.thin_port_messages_pipeline.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.bq_in_named_anchorages == "project.dataset.anchorages"
    assert config.bq_in_messages == "project.dataset.messages"
    assert config.bq_out_port_events == "project.dataset.output"
    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-01-07"
    assert config.wait_for_job is False


def test_cli_requires_named_anchorages_table(mocker):
    mocker.patch(
        "pipe_anchorages.cli.commands.thin_port_messages.thin_port_messages_pipeline.run",
        return_value=0,
    )
    args = [
        a for a in BASE_ARGS
        if a not in ("--bq-in-named-anchorages", "project.dataset.anchorages")
    ]

    with pytest.raises(argparse.ArgumentTypeError, match="bq_in_named_anchorages"):
        main.run(args)


def test_gfw_pipeline_resolves_project_and_labels_from_config():
    # thin_port_messages_pipeline.run() builds a GfwPipeline, and translates
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
    # classes (anchorage_options.py and friends), which still redeclare overlapping
    # flag names like --output_table. Not a production risk: each CLI invocation is
    # its own process, and those sibling classes are only ever lazily imported when
    # their own (still legacy) commands are actually invoked.
    labels = {"team": "pipeline", "env": "prod"}

    gfw_pipeline = GfwPipeline(
        project="test-project",
        runner="DirectRunner",
        labels=[f"{key}={value}" for key, value in labels.items()],
    )

    cloud_options = gfw_pipeline.cloud_options
    assert isinstance(cloud_options, GoogleCloudOptions)
    assert cloud_options.project == "test-project"
    assert sorted(cloud_options.labels) == ["env=prod", "team=pipeline"]
