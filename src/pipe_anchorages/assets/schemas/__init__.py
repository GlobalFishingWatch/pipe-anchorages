from importlib.resources import files

from gfw.common.io import json_load


def get_schema(filename: str) -> list[dict]:
    return json_load(files("pipe_anchorages.assets.schemas").joinpath(filename))
