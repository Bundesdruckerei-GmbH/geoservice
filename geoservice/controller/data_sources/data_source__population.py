from typing import Optional, NamedTuple

import numpy
import pandas
from geopandas import GeoDataFrame

import geoservice
from geoservice.constants import RESOURCES_PATH
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.model.geoobject import Population
from geoservice.utils.minio import MinioParquetHelper, MinioParquetConfig


class DataSourcePopulation(DataSourceBase):

    QUALITIES = {}
    LOCAL_STORAGE_PATH = RESOURCES_PATH / 'population' / 'population.parquet'
    ENGINE = 'pyarrow'
    MODEL = Population

    @classmethod
    def _extract_local(cls, qualities: Optional[NamedTuple] = None, **kwargs) -> GeoDataFrame:
        local_storage_path = cls.LOCAL_STORAGE_PATH or cls._local_storage_path(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        if not local_storage_path.exists():
            if geoservice.app.config.get('ETL_PULL_MISSING_FILES_FOR_LOCAL_RUNTIME'):
                cls.logger.info(f'{local_storage_path} not found')
                gdf = cls._extract_remote(qualities)
                gdf.to_parquet(path=str(local_storage_path), engine=cls.ENGINE)
                return gdf
            else:
                cls.logger.error(f'{local_storage_path} not found. Aborting')
        else:
            cls.logger.info(f'Using {local_storage_path}')
        # - - - - - - - - - - - - - - - - - - - -
        return pandas.read_parquet(local_storage_path, engine=cls.ENGINE)

    @classmethod
    def _extract_remote(cls, qualities: Optional[NamedTuple] = None, **kwargs) -> GeoDataFrame:
        minio_storage_path = cls._cfg_remote_storage_path() or cls._remote_storage_path(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        cls.logger.info('Pulling from Minio...')
        with MinioParquetHelper(
            objectname=minio_storage_path,
            config=MinioParquetConfig(
                access_key=cls._cfg_remote_access_key(),
                secret_key=cls._cfg_remote_secret_key(),
                bucket=cls._cfg_remote_bucket()
            )
        ) as gdf:
            return gdf

    @classmethod
    def _transform(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> GeoDataFrame:
        gdf = gdf[gdf["ISO3 Alpha-code"].notna()].copy()
        # - - - - - - - - - - - - - - - - - - - -
        gdf["value"] = gdf.loc[:, "0":"100+"].apply(
            lambda y: int(numpy.sum([float(x) for x in y.values.tolist()]) * 1000), axis=1
        )
        # - - - - - - - - - - - - - - - - - - - -
        gdf["source"] = "WPP2022"
        # - - - - - - - - - - - - - - - - - - - -
        gdf = gdf[["ISO3 Alpha-code", "value", "Year", "source"]].rename(
            columns={
                "ISO3 Alpha-code": "adm0_code",
                "Year": "year"
            }
        )
        # - - - - - - - - - - - - - - - - - - - -
        cls._sql_update_metadatastate('population', qualities)
        return gdf
