"""Encapsulates the anchorage position messages query."""

from datetime import date
from functools import cached_property
from typing import NamedTuple

from gfw.common.query import Query


class AnchorageMessage(NamedTuple):
    """Output type of the anchorage position messages query."""

    ident: str
    lat: float
    lon: float
    timestamp: float
    destination: str
    speed: float


class AnchorageLocationsQuery(Query):
    """Encapsulates the anchorage position messages query."""

    NAME = "anchorage_locations"
    JINJA_TEMPLATE_FILENAME = "anchorage_locations.sql.j2"

    def __init__(self, source_messages: str, start_date: date, end_date: date):
        self._source_messages = source_messages
        self._start_date = start_date
        self._end_date = end_date

    @cached_property
    def output_type(self) -> type[NamedTuple]:
        return AnchorageMessage

    @cached_property
    def template_filename(self) -> str:
        return self.JINJA_TEMPLATE_FILENAME

    @cached_property
    def template_vars(self) -> dict:
        return {
            "source_messages": self._source_messages,
            "start_date": self._start_date,
            "end_date": self._end_date,
        }
