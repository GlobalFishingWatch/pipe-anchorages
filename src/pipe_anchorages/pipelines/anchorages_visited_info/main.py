import logging
from types import SimpleNamespace
from typing import Callable
from functools import cached_property

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_anchorages.version import __version__
from pipe_anchorages.pipelines.anchorages_visited_info.config import AnchoragesVisitedInfoConfig
from pipe_anchorages.pipelines.anchorages_visited_info.table_config import (
    AnchoragesVisitedInfoTableConfig,
    AnchoragesVisitedInfoTableDescription
)

logger = logging.getLogger(__name__)


class AnchoragesVisitedInfoQuery(Query):
    def __init__(self, config: AnchoragesVisitedInfoConfig) -> None:
        self.config = config

    @cached_property
    def template_filename(self) -> str:
        return "anchorages_visited_info.sql.j2"

    @cached_property
    def template_vars(self) -> dict:
        return {
            "source_loitering": self.config.bq_input_loitering,
            "source_encounters": self.config.bq_input_encounters,
            "source_ais_gaps": self.config.bq_input_ais_gaps,
            "source_named_anchorages": self.config.bq_input_named_anchorages,
        }


def run(
    config: SimpleNamespace,
    unknown_unparsed_args: tuple = (),
    unknown_parsed_args: dict = None,
    bq_client_factory: Callable = None,
) -> None:

    config = AnchoragesVisitedInfoConfig.from_namespace(
        config,
        version=__version__,
        name="pipe-anchorages--anchorages-visited-info"
    )

    bq_client_factory = bq_client_factory or BigQueryHelper.get_client_factory(
        mocked=config.mock_bq_clients
    )

    query = AnchoragesVisitedInfoQuery(config)

    bq = BigQueryHelper(bq_client_factory, dry_run=config.dry_run, project=config.project)

    table_config = AnchoragesVisitedInfoTableConfig(
        table_id=config.bq_output,
        description=AnchoragesVisitedInfoTableDescription(
            version=__version__,
            relevant_params={}
        ),
    )

    # This pattern is not good. If something fails afterwards, we are left with no data.
    # But using WRITE_DISPOSITION=True removes the schema.
    # And updating the schema afterwards fails because there are REQUIRED fields.
    # TODO: Move to a pattern in which we write into a temporary table and then copy.
    logger.info(f"Re-creating events table '{config.bq_output}'...")
    bq.client.delete_table(config.bq_output, not_found_ok=True)
    bq.create_table(**table_config.to_bigquery_params(), labels=config.labels)

    logger.info('Executing anchorages visited info query...')
    query_result = bq.run_query(query.render(), config.bq_output, labels=config.labels)

    _ = query_result.query_job.result()
    logger.info("Done.")
