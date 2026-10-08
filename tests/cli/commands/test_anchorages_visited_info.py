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
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run():
    main.run(BASE_ARGS)
