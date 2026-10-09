from types import SimpleNamespace

from pipe_anchorages.assets import schemas
from pipe_anchorages.pipelines.anchorages_visited_info.main import run

SCHEMA_COLUMNS = [f["name"] for f in schemas.get_schema("anchorages_visited_info.json")]


def make_config(**overrides):
    config = dict(
        bq_input_loitering="project.dataset.loitering",
        bq_input_encounters="project.dataset.encounters",
        bq_input_ais_gaps="project.dataset.ais_gaps",
        bq_input_named_anchorages="project.dataset.named_anchorages",
        bq_output="project.dataset.anchorages_visited_info",
        project="test-project",
        labels={"environment": "development", "stage": "anchorages"},
    )
    config.update(overrides)
    return SimpleNamespace(**config)


def run_pipeline(bq_client_factory, bq_clients, **overrides):
    run(make_config(**overrides), bq_client_factory=bq_client_factory)
    (client,) = bq_clients
    return client


def test_run_writes_the_query_into_the_output_table(bq_client_factory, bq_clients):
    client = run_pipeline(bq_client_factory, bq_clients)

    client.query.assert_called_once()
    query_str = client.query.call_args.args[0]
    job_config = client.query.call_args.kwargs["job_config"]
    for table in ("loitering", "encounters", "ais_gaps", "named_anchorages"):
        assert f"project.dataset.{table}" in query_str
    assert job_config.destination.table_id == "anchorages_visited_info"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert job_config.labels == {"environment": "development", "stage": "anchorages"}


def test_run_leaves_the_output_table_unpartitioned_like_production(bq_client_factory, bq_clients):
    client = run_pipeline(bq_client_factory, bq_clients)

    job_config = client.query.call_args.kwargs["job_config"]
    assert job_config.time_partitioning is None
    assert job_config.clustering_fields is None


def test_run_sets_schema_description_and_labels_on_the_output_table(bq_client_factory, bq_clients):
    client = run_pipeline(bq_client_factory, bq_clients)

    client.update_table.assert_called_once()
    table, fields = client.update_table.call_args.args
    assert fields == ["schema", "description", "labels"]
    assert [f["name"] for f in table.schema] == SCHEMA_COLUMNS
    assert "ANCHORAGES VISITED INFO" in table.description
    assert table.labels == {"environment": "development", "stage": "anchorages"}


def test_run_with_dry_run_uses_a_dry_run_client(bq_client_factory, bq_clients, bq_factory_kwargs):
    run_pipeline(bq_client_factory, bq_clients, dry_run=True)

    (kwargs,) = bq_factory_kwargs
    assert kwargs["default_query_job_config"].dry_run is True
