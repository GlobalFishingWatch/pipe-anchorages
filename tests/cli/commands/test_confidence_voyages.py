import argparse

import pytest

from pipe_anchorages.cli import main


BASE_ARGS = [
    "confidence-voyages",
    "--bq-in-port-visits", "project.dataset.port_visits",
    "--min-confidence", "3",
    "--bq-out-voyages", "project.dataset.voyages_c3",
    "--project", "test-project",
    "--mock-bq-clients",
    "--labels", "environment=development", "stage=anchorages",
]


def test_cli_executes_run():
    main.run(BASE_ARGS)


@pytest.mark.parametrize("flag", ["--bq-in-port-visits", "--min-confidence", "--bq-out-voyages"])
def test_cli_requires_argument(flag):
    args = list(BASE_ARGS)
    i = args.index(flag)
    del args[i:i + 2]

    with pytest.raises(argparse.ArgumentTypeError, match="Missing required arguments"):
        main.run(args)
