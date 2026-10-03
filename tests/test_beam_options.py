from types import SimpleNamespace

from apache_beam.options.pipeline_options import GoogleCloudOptions

from pipe_anchorages.beam_options import build_pipeline_options
from pipe_anchorages.options.thin_port_messages_options import ThinPortMessagesOptions


FIELDS = [
    "bq_in_named_anchorages",
    "bq_in_messages",
    "bq_out_port_events",
    "start_date",
    "end_date",
    "config",
    "ssvid_filter",
    "wait_for_job",
]


def base_config(**overrides):
    config = dict(
        bq_in_named_anchorages="project.dataset.anchorages",
        bq_in_messages="project.dataset.messages",
        bq_out_port_events="project.dataset.output",
        start_date="2024-01-01",
        end_date="2024-01-07",
        config=None,
        ssvid_filter=None,
        wait_for_job=False,
        labels={},
        unknown_unparsed_args=[],
    )
    config.update(overrides)
    return SimpleNamespace(**config)


def test_build_pipeline_options_roundtrips_known_fields():
    config = base_config(config="path/to/config.yaml", wait_for_job=True)

    options = build_pipeline_options(config, FIELDS)
    known = options.view_as(ThinPortMessagesOptions)

    assert known.bq_in_named_anchorages == "project.dataset.anchorages"
    assert known.bq_in_messages == "project.dataset.messages"
    assert known.bq_out_port_events == "project.dataset.output"
    assert known.start_date == "2024-01-01"
    assert known.end_date == "2024-01-07"
    assert known.config == "path/to/config.yaml"
    assert known.ssvid_filter is None
    assert known.wait_for_job is True


def test_build_pipeline_options_omits_false_bool_field():
    config = base_config(wait_for_job=False)

    options = build_pipeline_options(config, FIELDS)
    known = options.view_as(ThinPortMessagesOptions)

    assert known.wait_for_job is False


def test_build_pipeline_options_translates_labels_dict_to_beam_native_flags():
    config = base_config(labels={"team": "pipeline", "env": "prod"})

    options = build_pipeline_options(config, FIELDS)
    cloud = options.view_as(GoogleCloudOptions)

    assert sorted(cloud.labels) == ["env=prod", "team=pipeline"]


def test_build_pipeline_options_passes_through_unknown_args():
    config = base_config(
        unknown_unparsed_args=["--project", "test-project", "--runner", "DirectRunner"],
    )

    options = build_pipeline_options(config, FIELDS)
    cloud = options.view_as(GoogleCloudOptions)

    assert cloud.project == "test-project"
