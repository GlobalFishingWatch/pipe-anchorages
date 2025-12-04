from dataclasses import dataclass

from pipe_anchorages.assets import schemas

from gfw.common.bigquery.table_config import TableConfig
from gfw.common.bigquery.table_description import TableDescription


SUMMARY = """\
A reference table of all anchorages enriched with indicators
showing their presence across multiple product event tables.
Each boolean column corresponds to a product's events dataset
and is set to TRUE when the anchorage appears in that dataset.

For more information, see https://github.com/GlobalFishingWatch/pipe-anchorages.
"""  # noqa

CAVEATS = """\
"""  # noqa


@dataclass
class AnchoragesVisitedInfoTableDescription(TableDescription):
    repo_name: str = "pipe-anchorages"
    title: str = "ANCHORAGES VISITED INFO"
    subtitle: str = "Anchorage reference table with boolean cross-dataset presence indicators."
    summary: str = SUMMARY
    caveats: str = CAVEATS


@dataclass
class AnchoragesVisitedInfoTableConfig(TableConfig):
    schema_file: str = "anchorages_visited_info.json"

    @property
    def schema(self) -> list[dict]:
        return schemas.get_schema(self.schema_file)
