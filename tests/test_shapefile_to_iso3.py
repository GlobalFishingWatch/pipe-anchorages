from importlib.resources import files

from pipe_anchorages.assets.data import EEZ
from pipe_anchorages.shapefile_to_iso3 import Iso3Finder


def test():
    items = [
        (37.7749, -140, None),
        (37.7749, -122.4194, "USA"),
        (31.2304, 121.4737, "CHN"),
        (51.5074, -0.1278, "GBR"),
        (-33.8688, 151.2093, "AUS"),
    ]
    finder = Iso3Finder(files(EEZ).joinpath("EEZ_Land_v3_202030.shp"))
    found = []
    expected = []
    for lat, lon, iso3 in items:
        found.append(finder.iso3(lat, lon))
        expected.append(iso3)

    assert expected == found


test()
