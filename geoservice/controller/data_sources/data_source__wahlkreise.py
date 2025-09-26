# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from typing import NamedTuple, Optional

from geopandas import GeoDataFrame
from sqlalchemy import select

from geoservice.constants import RESOURCES_PATH
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.model import db
from geoservice.model.geoobject import Wahlkreise, Adm1


class DataSourceWahlkreise(DataSourceBase):
    QUALITIES = {}
    LOCAL_STORAGE_PATH = RESOURCES_PATH / 'wahlkreise' / 'wahlkreise_deu.gpkg'
    MODEL = Wahlkreise

    @classmethod
    def _transform(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> GeoDataFrame:
        gdf.rename(columns=str.lower, inplace=True)
        # - - - - - - - - - - - - - - - - - - - -
        for col in ["wkr_nr", "land_nr"]:
            gdf[col] = gdf[col].astype('int64')
        # - - - - - - - - - - - - - - - - - - - -
        gdf[["source", "adm1_code"]] = ["wahlkreise", None]
        # - - - - - - - - - - - - - - - - - - - -
        cls.logger.info(f"Linking districts of data wahlkreise with adm1_code")
        country_name_to_adm1_code = {
            country_name: adm1_code
            for country_name, adm1_code in
            db.session.execute(
                select(Adm1.name, Adm1.adm1_code)
                .where(Adm1.source == 'gadm')
            ).all()
        }
        for idx, row in gdf.iterrows():
            try:
                gdf.loc[gdf.index[idx], "adm1_code"] = country_name_to_adm1_code[row["land_name"]]
            except KeyError:
                cls.logger.debug(f"No match found for district {gdf.iloc[idx]['land_name']}")
        # - - - - - - - - - - - - - - - - - - - -
        cls._sql_update_bbox('wahlkreise', gdf, qualities)
        cls._sql_update_crs('wahlkreise', gdf, qualities)
        cls._sql_update_metadatastate('wahlkreise', qualities)
        return gdf
