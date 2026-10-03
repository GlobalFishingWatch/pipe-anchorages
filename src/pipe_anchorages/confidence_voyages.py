import logging
import time

from importlib.resources import files
from types import SimpleNamespace
from typing import Any

from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from pipe_anchorages.utils.bqtools import BigQueryHelper, DatePartitionedTable, Schemas
from pipe_anchorages.utils.ver import get_pipe_ver

logger = logging.getLogger()

confidence_meaning = {
    "1": "no stop or gap; only an entry and/or exit",
    "2": "only stop and/or gap; no entry or exit",
    "3": "port entry or exit with stop and/or gap",
    "4": "port entry and exit with stop and/or gap",
}

DROP_VOYAGES_QUERY = """
DELETE FROM `{table_id}`
WHERE date({partitioning_field}) >= '1970-01-01' or {partitioning_field} is null
"""


ASSETS_DIR = "pipe_anchorages.assets"
SCHEMA_FILENAME = "generate_confidence_voyages.json"
QUERY_FILENAME = "generate_confidence_voyages.sql.j2"

SCHEMA_PATH = files(f"{ASSETS_DIR}.schemas").joinpath(SCHEMA_FILENAME)
QUERIES_DIR = files(f"{ASSETS_DIR}.queries")

env_j2 = Environment(loader=FileSystemLoader(QUERIES_DIR))


def run(config: SimpleNamespace, **kwargs: Any) -> None:
    start_time = time.time()

    labels = config.labels or {}

    bq_helper = BigQueryHelper(
        bq_client=bigquery.Client(
            project=config.project,
        ),
        labels=labels,
    )

    # 1. Validate the existance of the table
    logging.info(f"Creates the confidence voyages table <{config.output}> if it does not exists")
    table = DatePartitionedTable(
        table_id=config.output,
        description=f"""
            Created by pipe-anchorages: {get_pipe_ver()}.
            * Create voyages filter per minimal confidence.
            * https://github.com/GlobalFishingWatch/pipe-research
            * Source: {config.source}
            * Minimal confidence: {config.min_confidence} meaning: {confidence_meaning[config.min_confidence]}.

            A "voyage" is defined as the combination of a vessel's previous port_visit's end and next port_visit's start.
            Every vessel's first voyage has an unknown start, so the `trip_start_*` columns are NULL. Respectively, each vessel's last voyage has an undefined end, so the `trip_end_*` columns are NULL.
            If you want to include a vessel's first (or last) voyage you will have to adjust the trip_start (or trip_end) filter to also include NULL values, e.g:
            ...
            WHERE (trip_start <= '2022-12-31' OR trip_start IS NULL)
        """,  # noqa: E501
        schema=Schemas.load_json_schema(SCHEMA_PATH),
        partitioning_field="trip_start",
    )
    bq_helper.ensure_table_exists(table)
    bq_helper.run_query(
        query=DROP_VOYAGES_QUERY.format(
            table_id=table.table_id,
            partitioning_field=table.partitioning_field,
        )
    )
    bq_helper.update_table(table)

    # Apply template
    template = env_j2.get_template(QUERY_FILENAME)
    query = template.render(
        {
            "port_visits_table": f"{config.source}",
            "min_confidence": config.min_confidence,
        }
    )
    # Run query and calc research positions
    bq_helper.run_query_into_table(
        query=query,
        table=table,
    )

    # ALL DONE
    logger.info(f"All done, you can find the output: {config.output}")
    logger.info(f"Execution time {(time.time()-start_time)/60} minutes")
