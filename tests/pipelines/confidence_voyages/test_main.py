from types import SimpleNamespace

from pipe_anchorages.assets import schemas
from pipe_anchorages.pipelines.confidence_voyages.main import ConfidenceVoyagesQuery, run

SCHEMA_COLUMNS = [f["name"] for f in schemas.get_schema("confidence_voyages.json")]


def make_config(**overrides):
    config = dict(
        bq_in_port_visits="project.dataset.port_visits",
        bq_out_voyages="project.dataset.voyages_c3",
        min_confidence="3",
        project="test-project",
        labels={"team": "pipeline", "env": "dev"},
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
    assert "project.dataset.port_visits" in query_str
    assert "confidence >= 3" in query_str
    assert job_config.destination.table_id == "voyages_c3"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert job_config.labels == {"team": "pipeline", "env": "dev"}


def test_run_creates_the_output_table_partitioned_like_production(bq_client_factory, bq_clients):
    client = run_pipeline(bq_client_factory, bq_clients)

    job_config = client.query.call_args.kwargs["job_config"]
    assert job_config.time_partitioning.type_ == "MONTH"
    assert job_config.time_partitioning.field == "trip_start"
    assert job_config.clustering_fields == ["trip_start"]


def test_run_sets_schema_description_and_labels_on_the_output_table(bq_client_factory, bq_clients):
    client = run_pipeline(bq_client_factory, bq_clients)

    client.update_table.assert_called_once()
    table, fields = client.update_table.call_args.args
    assert fields == ["schema", "description", "labels"]
    assert [f["name"] for f in table.schema] == SCHEMA_COLUMNS
    assert "min_confidence" in table.description
    assert table.labels == {"team": "pipeline", "env": "dev"}


def test_query_outputs_columns_in_schema_order():
    rendered = ConfidenceVoyagesQuery(make_config()).render()

    voyages = rendered[rendered.index("bq_voyages AS ("):]
    start = voyages.index("SELECT") + len("SELECT")
    select = voyages[start:voyages.index("FROM port_visit_rownumber")]
    columns = [c.strip().split()[-1] for c in split_top_level_commas(select)]

    assert columns == SCHEMA_COLUMNS


def split_top_level_commas(text):
    """Split on commas that aren't inside parentheses."""
    parts, depth, start = [], 0, 0
    for i, ch in enumerate(text):
        depth += {"(": 1, ")": -1}.get(ch, 0)
        if ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return [p for p in parts if p.strip()]
