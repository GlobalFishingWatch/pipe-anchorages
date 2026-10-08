from types import SimpleNamespace

import pytest

from gfw.common.bigquery.helper import BigQueryHelper

from pipe_anchorages.assets import schemas
from pipe_anchorages.pipelines.confidence_voyages.main import ConfidenceVoyagesQuery, run

SCHEMA_COLUMNS = [f["name"] for f in schemas.get_schema("confidence_voyages.json")]


@pytest.fixture
def clients():
    """Mock BigQuery clients created by the pipeline, to inspect what it sent to BigQuery."""
    return []


@pytest.fixture
def client_factory(clients):
    mock_factory = BigQueryHelper.get_client_factory(mocked=True)

    def factory(**kwargs):
        client = mock_factory(**kwargs)
        clients.append(client)
        return client

    return factory


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


def run_pipeline(client_factory, clients, **overrides):
    run(make_config(**overrides), bq_client_factory=client_factory)
    (client,) = clients
    return client


def test_run_writes_the_query_into_the_output_table(client_factory, clients):
    client = run_pipeline(client_factory, clients)

    client.query.assert_called_once()
    query_str = client.query.call_args.args[0]
    job_config = client.query.call_args.kwargs["job_config"]
    assert "project.dataset.port_visits" in query_str
    assert "confidence >= 3" in query_str
    assert job_config.destination.table_id == "voyages_c3"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert job_config.labels == {"team": "pipeline", "env": "dev"}


def test_run_creates_the_output_table_partitioned_like_production(client_factory, clients):
    client = run_pipeline(client_factory, clients)

    job_config = client.query.call_args.kwargs["job_config"]
    assert job_config.time_partitioning.type_ == "MONTH"
    assert job_config.time_partitioning.field == "trip_start"
    assert job_config.clustering_fields == ["trip_start"]


def test_run_sets_schema_description_and_labels_on_the_output_table(client_factory, clients):
    client = run_pipeline(client_factory, clients)

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
