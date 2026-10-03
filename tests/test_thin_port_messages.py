import argparse

import pytest

from pipe_anchorages.cli import main
from pipe_anchorages.options.thin_port_messages_options import ThinPortMessagesOptions


BASE_ARGS = [
    "thin-port-messages",
    "--anchorage-table", "project.dataset.anchorages",
    "--input-table", "project.dataset.messages",
    "--output-table", "project.dataset.output",
    "--start-date", "2024-01-01",
    "--end-date", "2024-01-07",
]


def test_cli_executes_run(mocker):
    mock_run = mocker.patch(
        "pipe_anchorages.cli.commands.thin_port_messages.thin_port_messages_pipeline.run",
        return_value=0,
    )

    main.run(BASE_ARGS)

    mock_run.assert_called_once()
    options = mock_run.call_args[0][0]
    known = options.view_as(ThinPortMessagesOptions)
    assert known.anchorage_table == "project.dataset.anchorages"
    assert known.input_table == "project.dataset.messages"
    assert known.output_table == "project.dataset.output"
    assert known.start_date == "2024-01-01"
    assert known.end_date == "2024-01-07"


def test_cli_requires_anchorage_table(mocker):
    mocker.patch(
        "pipe_anchorages.cli.commands.thin_port_messages.thin_port_messages_pipeline.run",
        return_value=0,
    )
    args = [a for a in BASE_ARGS if a not in ("--anchorage-table", "project.dataset.anchorages")]

    with pytest.raises(argparse.ArgumentTypeError, match="anchorage_table"):
        main.run(args)
