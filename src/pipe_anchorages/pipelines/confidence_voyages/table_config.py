import textwrap
from dataclasses import dataclass

from pipe_anchorages.assets import schemas

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


def collapse_paragraphs(text: str) -> str:
    """Collapse paragraphs with arbitrary newlines ('\n') into single lines."""
    paragraphs = textwrap.dedent(text).strip().split("\n\n")  # preserve paragraphs.
    cleaned = [" ".join(p.split()) for p in paragraphs]
    return "\n\n".join(cleaned)


CONFIDENCE_MEANING = {
    "1": "no stop or gap; only an entry and/or exit",
    "2": "only stop and/or gap; no entry or exit",
    "3": "port entry or exit with stop and/or gap",
    "4": "port entry and exit with stop and/or gap",
}

SUMMARY = """\
A "voyage" is the combination of a vessel's previous port visit's end and its next
port visit's start.
"""

CAVEATS = """\
Every vessel's first voyage has an unknown start, so the trip_start_* columns are NULL.
Likewise, each vessel's last voyage has an undefined end, so the trip_end_* columns are NULL.
If you want to include a vessel's first (or last) voyage, adjust the trip_start (or
trip_end) filter to also include NULL values, e.g.:
WHERE (trip_start <= '2022-12-31' OR trip_start IS NULL)
"""


@dataclass
class ConfidenceVoyagesTableDescription(TableDescription):
    repo_name: str = "pipe-anchorages"
    title: str = "CONFIDENCE VOYAGES"
    subtitle: str = "Voyages between consecutive port visits, filtered by minimum confidence"
    summary: str = collapse_paragraphs(SUMMARY)
    caveats: str = collapse_paragraphs(CAVEATS)


@dataclass
class ConfidenceVoyagesTableConfig(TableConfig):
    schema_file: str = "confidence_voyages.json"
    # Same layout as the production voyages_c* tables.
    partition_type: str = "MONTH"
    partition_field: str = "trip_start"
    clustering_fields: tuple[str, ...] = ("trip_start",)

    @property
    def schema(self) -> list[dict]:
        return schemas.get_schema(self.schema_file)
