import logging
from datetime import datetime
from types import SimpleNamespace
from typing import Callable
from functools import cached_property

from google.api_core.exceptions import NotFound, Conflict

from gfw.common.bigquery.helper import BigQueryHelper
from gfw.common.query import Query

from pipe_anchorages.version import __version__
from pipe_anchorages.pipelines.anchorages_visited_info.config import AnchoragesVisitedInfoConfig
from pipe_anchorages.pipelines.anchorages_visited_info.table_config import (
    AnchoragesVisitedInfoTableConfig,
    AnchoragesVisitedInfoTableDescription
)

logger = logging.getLogger(__name__)

RENAME_QUERY = "ALTER TABLE `{old}` RENAME TO `{new}`"
SUFFIX_STAGING = "_staging_{id}"
SUFFIX_BACKUP = "_backup"


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

    staging_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")

    staging_table_id = config.bq_output + SUFFIX_STAGING.format(id=staging_id)
    backup_table_id = config.bq_output + SUFFIX_BACKUP
    prod_table_id = config.bq_output

    table_config = AnchoragesVisitedInfoTableConfig(
        table_id=staging_table_id,
        description=AnchoragesVisitedInfoTableDescription(
            version=__version__,
            relevant_params={}
        ),
    )

    logger.info(f"Creating staging table '{staging_table_id}...")
    bq.create_table(**table_config.to_bigquery_params(), labels=config.labels)

    logger.info('Executing anchorages visited info query...')
    query_result = bq.run_query(
        query.render(),
        staging_table_id,
        labels=config.labels,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_NEVER",
    )
    _ = query_result.query_job.result()

    logger.info(f"Renaming prod table to backup: {backup_table_id}...")
    try:
        query = RENAME_QUERY.format(old=prod_table_id, new=backup_table_id.split(".")[-1])
        query_result = bq.run_query(query)
        _ = query_result.query_job.result()
    except (NotFound, Conflict):
        # If prod doesn't exist, just skip
        pass

    logger.info("Renaming staging table to prod...")
    query = RENAME_QUERY.format(old=staging_table_id, new=prod_table_id.split(".")[-1])
    query_result = bq.run_query(query)
    _ = query_result.query_job.result()

    logger.info("Deleting backup table...")
    bq.client.delete_table(backup_table_id, not_found_ok=True)
    logger.info("Done.")
