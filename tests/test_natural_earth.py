import pytest
import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from geoservice.controller.natural_earth import transform


@pytest.mark.skip(reason="temporarily disabled for deploy")
def test_transform():
    adm1_ne = gpd.GeoDataFrame(
        {
            "name": ["Test"],
            "geometry-level": [0],
            "adm0_a3": ["Test0"],
            "adm1_code": ["Test1"],
            "geometry": Polygon(((0, 0), (0, 1), (1, 1), (1, 0), (0, 0)))
        }
        )

    adm0_ne = pd.DataFrame(
        {
            "ISO_A3": ["Test0"],
            "ADM0_A3_DE": ["Test0"],
            "NAME": ["Test"],
        }
    )
    adm1_ne_highres, adm1_ne_medres, adm1_ne_lowres, adm0_ne_highres, adm0_ne_medres, adm0_ne_lowres = transform(adm1_ne,adm0_ne)


    tester = gpd.GeoDataFrame(
        {
            "name": ["Test"],
            "geometry-level": [0],
            "geometry": Polygon(((0, 0), (0, 1), (1, 1), (1, 0), (0, 0))),
            "adm0-Code": ["Test0"],
            "adm1-Code": ["Test1"]
        }
        )
    assert adm1_ne_highres.equals(tester)

