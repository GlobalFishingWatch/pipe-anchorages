import argparse
import logging

from types import SimpleNamespace

import pytest

from pipe_anchorages import name_anchorages_pipeline
from pipe_anchorages.cli import main


BASE_ARGS = [
    "named-anchorages",
    "--bq-in-anchorage-points", "project.dataset.anchorage_points",
    "--bq-out-named-anchorages", "project.dataset.named_anchorages",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.named_anchorages.name_anchorages_pipeline.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    config = mock_run.call_args[0][0]
    assert config.bq_in_anchorage_points == "project.dataset.anchorage_points"
    assert config.bq_out_named_anchorages == "project.dataset.named_anchorages"
    assert config.shapefile == "EEZ_Land_v3_202030.shp"
    assert config.label_distance_km == 4.0
    assert config.sublabel_distance_km == 1.0
    assert config.anchorage_overrides == "anchorage_overrides.csv"
    assert config.reference_ports == ["peru.csv", "indonesia.csv", "WPI_ports.csv"]
    assert config.reference_places == ["geonames_1000.csv"]


def test_cli_requires_bq_out_named_anchorages(mocker):
    mocker.patch(
        "pipe_anchorages.cli.commands.named_anchorages.name_anchorages_pipeline.run",
        return_value=0,
    )
    excluded = ("--bq-out-named-anchorages", "project.dataset.named_anchorages")
    args = [a for a in BASE_ARGS if a not in excluded]

    with pytest.raises(argparse.ArgumentTypeError, match="bq_out_named_anchorages"):
        main.run(args)


def test_run_forwards_config_file_beam_options_to_pipeline(mocker):
    # Same forwarding contract as the other migrated pipelines: config-file keys
    # that aren't one of this command's own Options land in config.unknown_parsed_args,
    # and run() must forward that dict into Pipeline(**options), matching pipe-gaps'
    # PipelineFactory.
    #
    # Only patches Pipeline itself, not the Beam transforms downstream -- those build
    # a real DAG against a mocked Pipeline and fail past the point this test cares
    # about (e.g. apache_beam's own pickling of a DoFn against a Mock), which is
    # expected and ignored here.
    mock_pipeline_cls = mocker.patch("pipe_anchorages.name_anchorages_pipeline.Pipeline")

    config = SimpleNamespace(
        bq_in_anchorage_points="project.dataset.anchorage_points",
        bq_out_named_anchorages="project.dataset.named_anchorages",
        shapefile="EEZ_Land_v3_202030.shp",
        label_distance_km=4.0,
        sublabel_distance_km=1.0,
        anchorage_overrides="anchorage_overrides.csv",
        reference_ports=["peru.csv", "indonesia.csv", "WPI_ports.csv"],
        reference_places=["geonames_1000.csv"],
        labels={"team": "pipeline"},
        unknown_unparsed_args=[],
        unknown_parsed_args={"project": "test-project", "max_num_workers": 50},
    )

    try:
        # Silences the real (expected, ignored) DAG's own noisy logging -- see comment above.
        logging.disable(logging.CRITICAL)
        name_anchorages_pipeline.run(config)
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
