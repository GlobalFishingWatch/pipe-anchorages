import logging
from datetime import datetime
from types import SimpleNamespace
from typing import Callable
from functools import cached_property

from google.cloud import bigquery
from google.cloud import storage

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

    if bq_client_factory is None:
        bq_client_factory = BigQueryHelper.get_client_factory(config.mock_bq_clients)

    query = AnchoragesVisitedInfoQuery(config)
    bq = BigQueryHelper(bq_client_factory, dry_run=config.dry_run, project=config.project)
    gs = storage.Client(project=config.project)

    table_config = AnchoragesVisitedInfoTableConfig(
        table_id=config.bq_output,
        staging_suffix=config.bq_staging_suffix,
        description=AnchoragesVisitedInfoTableDescription(
            version=__version__,
            relevant_params={}
        ),
    )
    bucket_id = config.gcs_bucket
    run_id = datetime.now().date()

    gcs_prefix = f"{config.gcs_prefix}/{table_config.table_id}/{run_id}"
    gcs_uri = f"gs://{bucket_id}/{gcs_prefix}/*.parquet"

    logger.info("Removing temp GCS files...")
    bucket = gs.get_bucket(bucket_id)
    blobs = list(bucket.list_blobs(prefix=gcs_prefix))
    bucket.delete_blobs(blobs)

    logger.info(f"Running query into temp table: {table_config.staging_table_id}...")
    query_result = bq.run_query(
        query.render(),
        destination=table_config.staging_table_id,
        write_disposition="WRITE_TRUNCATE",
    )
    query_result.query_job.result()

    logger.info(f"Exporting temp table to GCS: {gcs_uri}...")
    extract_job = bq.client.extract_table(
        table_config.staging_table_id,
        gcs_uri,
        job_config=bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.PARQUET
        ),
    )
    extract_job.result()

    logger.info(f"Loading from GCS to final prod table: {table_config.table_id}...")
    load_job = bq.client.load_table_from_uri(
        gcs_uri,
        table_config.table_id,
        job_config=bigquery.LoadJobConfig(
            schema=table_config.schema,
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            source_format=bigquery.SourceFormat.PARQUET,
            schema_update_options=[],
        ),
    )
    load_job.result()
    logger.info("Done.")
