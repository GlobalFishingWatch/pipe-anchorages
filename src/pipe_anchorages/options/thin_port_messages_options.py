from __future__ import absolute_import
from importlib.resources import files

from apache_beam.options.pipeline_options import PipelineOptions

from pipe_anchorages.assets import config as config_pkg

default_config_file = files(config_pkg).joinpath("anchorage_cfg.yaml")


class ThinPortMessagesOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group("Required")
        optional = parser.add_argument_group("Optional")

        required.add_argument(
            "--bq-in-named-anchorages", required=True, help="Name of of anchorages table (BQ)"
        )
        required.add_argument(
            "--bq-in-messages", required=True, help="Table to pull position messages from"
        )
        required.add_argument(
            "--bq-out-port-events",
            required=True,
            help="Output table (BQ) to write results to.",
        )
        required.add_argument(
            "--start-date",
            required=True,
            help="First date to look for entry/exit events.",
        )
        required.add_argument(
            "--end-date",
            required=True,
            help="Last date (inclusive) to look for entry/exit events.",
        )

        optional.add_argument(
            "--config", default=str(default_config_file), help="Path to configuration file"
        )
        optional.add_argument(
            "--ssvid-filter",
            help="Subquery or list of ssvid to limit processing to.\n"
            "If prefixed by @, load from given path",
        )
        optional.add_argument(
            "--wait-for-job",
            default=False,
            action="store_true",
            help="Wait until the job finishes before returning.",
        )
