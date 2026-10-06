from pipe_anchorages.cli import main


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
