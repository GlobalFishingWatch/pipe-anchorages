import textwrap
from dataclasses import dataclass
from functools import cached_property

from pipe_anchorages.assets import schemas

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


def collapse_paragraphs(text: str) -> str:
    """Collapse paragraphs with arbitrary newlines ('\n') into single lines."""
    paragraphs = textwrap.dedent(text).strip().split("\n\n")  # preserve paragraphs.
    cleaned = [" ".join(p.split()) for p in paragraphs]
    return "\n\n".join(cleaned)


SUMMARY = """\
A reference table of all anchorages enriched with indicators showing their presence
across multiple product event tables. Each boolean column corresponds to a product
events table and is set to TRUE when the anchorage appears at least once as the vessel's
destination port in that table.
"""

CAVEATS = """\
⬖ To be completed.
"""


@dataclass
class AnchoragesVisitedInfoTableDescription(TableDescription):
    repo_name: str = "pipe-anchorages"
    title: str = "ANCHORAGES VISITED INFO"
    subtitle: str = "Anchorage reference table with boolean cross-dataset presence indicators"
    summary: str = collapse_paragraphs(SUMMARY)
    caveats: str = CAVEATS


@dataclass
class AnchoragesVisitedInfoTableConfig(TableConfig):
    schema_file: str = "anchorages_visited_info.json"

    @property
    def schema(self) -> list[dict]:
        return schemas.get_schema(self.schema_file)

    @cached_property
    def staging_table_id(self):
        return self.table_id + self.staging_suffix
