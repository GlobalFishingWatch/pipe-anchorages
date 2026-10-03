from pipe_anchorages.cli import main


BASE_ARGS = [
    "generate-confidence-voyages",
    "--bq-in-port-visits", "project.dataset.port_visits",
    "--min-confidence", "3",
    "--bq-out-voyages", "project.dataset.voyages_c3",
    "--project", "test-project",
]


def test_cli_executes_run(mocker):
    # The legacy BigQueryHelper this command still uses has no client-factory
    # injection (unlike gfw.common.bigquery.helper.BigQueryHelper), so there's no
    # --mock-bq-clients style flag to lean on -- patch bigquery.Client itself.
    mock_client = mocker.MagicMock()
    mocker.patch("pipe_anchorages.confidence_voyages.bigquery.Client", return_value=mock_client)

    main.run(BASE_ARGS)

    mock_client.create_table.assert_called_once()
    mock_client.query_and_wait.assert_called()
    mock_client.get_table.assert_called_once()
    mock_client.update_table.assert_called_once()
