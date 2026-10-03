from types import SimpleNamespace

from apache_beam.options.pipeline_options import GoogleCloudOptions

from pipe_anchorages.cli.beam_options import build_pipeline_options
from pipe_anchorages.options.thin_port_messages_options import ThinPortMessagesOptions


FIELDS = [
    "anchorage_table",
    "input_table",
    "output_table",
    "start_date",
    "end_date",
    "config",
    "ssvid_filter",
    "wait_for_job",
]


def test_build_pipeline_options_roundtrips_known_fields():
    config = SimpleNamespace(
        anchorage_table="project.dataset.anchorages",
        input_table="project.dataset.messages",
        output_table="project.dataset.output",
        start_date="2024-01-01",
        end_date="2024-01-07",
        config="path/to/config.yaml",
        ssvid_filter=None,
        wait_for_job=True,
        labels={},
        unknown_unparsed_args=[],
    )

    options = build_pipeline_options(config, FIELDS)
    known = options.view_as(ThinPortMessagesOptions)

    assert known.anchorage_table == "project.dataset.anchorages"
    assert known.input_table == "project.dataset.messages"
    assert known.output_table == "project.dataset.output"
    assert known.start_date == "2024-01-01"
    assert known.end_date == "2024-01-07"
    assert known.config == "path/to/config.yaml"
    assert known.ssvid_filter is None
    assert known.wait_for_job is True


def test_build_pipeline_options_omits_false_bool_field():
    config = SimpleNamespace(
        anchorage_table="a",
        input_table="b",
        output_table="c",
        start_date="2024-01-01",
        end_date="2024-01-07",
        config=None,
        ssvid_filter=None,
        wait_for_job=False,
        labels={},
        unknown_unparsed_args=[],
    )

    options = build_pipeline_options(config, FIELDS)
    known = options.view_as(ThinPortMessagesOptions)

    assert known.wait_for_job is False


def test_build_pipeline_options_translates_labels_dict_to_beam_native_flags():
    config = SimpleNamespace(
        anchorage_table="a",
        input_table="b",
        output_table="c",
        start_date="2024-01-01",
        end_date="2024-01-07",
        config=None,
        ssvid_filter=None,
        wait_for_job=False,
        labels={"team": "pipeline", "env": "prod"},
        unknown_unparsed_args=[],
    )

    options = build_pipeline_options(config, FIELDS)
    cloud = options.view_as(GoogleCloudOptions)

    assert sorted(cloud.labels) == ["env=prod", "team=pipeline"]


def test_build_pipeline_options_passes_through_unknown_args():
    config = SimpleNamespace(
        anchorage_table="a",
        input_table="b",
        output_table="c",
        start_date="2024-01-01",
        end_date="2024-01-07",
        config=None,
        ssvid_filter=None,
        wait_for_job=False,
        labels={},
        unknown_unparsed_args=["--project", "test-project", "--runner", "DirectRunner"],
    )

    options = build_pipeline_options(config, FIELDS)
    cloud = options.view_as(GoogleCloudOptions)

    assert cloud.project == "test-project"
