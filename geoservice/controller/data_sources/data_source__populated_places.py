# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from pathlib import Path
from typing import Optional, NamedTuple, Type

from geopandas import GeoDataFrame
from sqlalchemy import delete

from geoservice.model import db
from geoservice.constants import RESOURCES_PATH
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.model.geoobject import PopulatedPlaces


class DataSourcePopulatedPlaces(DataSourceBase):
    SOURCE = 'populated_places'
    LOCAL_STORAGE_PATH = RESOURCES_PATH / 'naturalearth' / str(SOURCE + '.gpkg')
    QUALITIES = {}
    COLUMNS_KEEP = [
        "FEATURECLA",
        "ADM0_A3",
        "NAMEASCII",
        "NAME_DE",
        "NAME_EN",
        "NAME_FR",
        "POP_MIN",
        "geometry",
    ]
    TARGET_ADMS = [
        "Admin-0 capital",
        "Admin-0 region capital",
        "Admin-1 capital",
        "Admin-1 region capital",
    ]

    @classmethod
    def _layer(cls, qualities: NamedTuple) -> str:
        return "ne_10m_populated_places"

    @classmethod
    def _local_storage_path(cls, qualities: NamedTuple) -> Path:
        return cls.LOCAL_STORAGE_PATH

    @classmethod
    def _remote_storage_path(cls, qualities: NamedTuple) -> str:
        return str(Path("Natural_Earth") / "v5.1.1" / "natural_earth_vector.gpkg")

    @classmethod
    def _model(cls, qualities: NamedTuple) -> Type[PopulatedPlaces]:
        return PopulatedPlaces

    @classmethod
    def _persist(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> None:
        """Persists the data in respect to the data source model class used.

        Previous states of the database table used are deleted before writing.

        Args:
            gdf: the processed dataset
            qualities: see _transform method

        Returns:

        """
        model = cls.MODEL or cls._model(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        db.session.execute(
            delete(model).where(
                model.source == cls.SOURCE,
            )
        )
        # - - - - - - - - - - - - - - - - - - - -
        db.session.add_all(list(map(lambda row: model(**row[1].to_dict()), gdf.iterrows())))
        db.session.commit()

    @classmethod
    def _transform(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> GeoDataFrame:
        """Condense dataset to national and subnational capitals, plus major cities.

        Sometimes, major cities are not national or subnational capitals. One example is New York City.
        For that reason also cities with a population above 1 million inhabitants where included into
        the condensed dataset.

        As population count "POP_MIN" is chosen, as it represents the population of city area.
        For more details, refer to \n
        https://www.naturalearthdata.com/blog/miscellaneous/natural-earth-v2-0-0-release-notes/#LC10

        In addition, the data set is thinned out, which greatly reduces the number of its attributes.

        Args:
            gdf: Base dataframe on which the transformation is applied
            qualities: parent class constant holding user input about simplification- and ADM level

        Returns: Processed dataframe
        """
        POPULATION_THRESHOLD = 1000000

        adm_mask = gdf['FEATURECLA'].isin(cls.TARGET_ADMS)
        population_mask = gdf['POP_MIN'] > POPULATION_THRESHOLD
        df = gdf[adm_mask | population_mask][cls.COLUMNS_KEEP]

        df['FEATURECLA'] = df['FEATURECLA'].replace(
            regex={
                r'^Admin-0.*$': 'adm0',
                r'^Admin-1.*$': 'adm1',
            },
        )
        df["source"] = cls.SOURCE

        df = df.rename(
            columns={
                'FEATURECLA': 'capital_level',
                'ADM0_A3': 'adm0_code',
                'POP_MIN': 'population',
            },
        )
        df.columns = map(
            str.lower,
            df.columns,
        )
        return df
