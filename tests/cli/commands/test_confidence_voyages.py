from types import SimpleNamespace

from gfw.common.bigquery.helper import BigQueryHelper

from pipe_anchorages.assets import schemas
from pipe_anchorages.cli import main
from pipe_anchorages.pipelines.confidence_voyages.main import ConfidenceVoyagesQuery


BASE_ARGS = [
    "confidence-voyages",
    "--bq-in-port-visits", "project.dataset.port_visits",
    "--min-confidence", "3",
    "--bq-out-voyages", "project.dataset.voyages_c3",
    "--project", "test-project",
    "--mock-bq-clients",
]


def test_cli_executes_run():
    main.run(BASE_ARGS)


def test_cli_writes_output_partitioned_like_production(mocker):
    run_query = mocker.spy(BigQueryHelper, "run_query")

    main.run(BASE_ARGS)

    run_query.assert_called_once()
    kwargs = run_query.call_args.kwargs
    assert kwargs["destination"] == "project.dataset.voyages_c3"
    assert kwargs["write_disposition"] == "WRITE_TRUNCATE"
    assert kwargs["time_partitioning"].type_ == "MONTH"
    assert kwargs["time_partitioning"].field == "trip_start"
    assert kwargs["clustering_fields"] == ["trip_start"]


def test_query_outputs_columns_in_schema_order():
    query = ConfidenceVoyagesQuery(
        SimpleNamespace(bq_in_port_visits="project.dataset.port_visits", min_confidence="3")
    )
    rendered = query.render()
    voyages = rendered[rendered.index("bq_voyages AS ("):]
    start = voyages.index("SELECT") + len("SELECT")
    select = voyages[start:voyages.index("FROM port_visit_rownumber")]
    columns = [c.strip().split()[-1] for c in split_top_level_commas(select)]

    assert columns == [f["name"] for f in schemas.get_schema("confidence_voyages.json")]


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
