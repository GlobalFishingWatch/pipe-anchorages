from gfw.common.bigquery.helper import BigQueryHelper

from pipe_anchorages.cli import main


BASE_ARGS = [
    "anchorages-visited-info",
    "--bq-input-loitering", "project.dataset.loitering",
    "--bq-input-encounters", "project.dataset.encounters",
    "--bq-input-ais-gaps", "project.dataset.ais_gaps",
    "--bq-input-named-anchorages", "project.dataset.named_anchorages",
    "--bq-output", "project.dataset.output",
    "--project", "test-project",
    "--mock-bq-clients",
]


def test_cli_executes_run():
    main.run(BASE_ARGS)


def test_cli_forwards_labels_to_query_job(mocker):
    run_query = mocker.spy(BigQueryHelper, "run_query")

    main.run([*BASE_ARGS, "--labels", "team=pipeline", "env=dev"])

    run_query.assert_called_once()
    assert run_query.call_args.kwargs["labels"] == {"team": "pipeline", "env": "dev"}
