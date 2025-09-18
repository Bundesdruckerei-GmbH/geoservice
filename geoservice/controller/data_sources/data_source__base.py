import logging
import warnings
from abc import ABC
from functools import partialmethod
from pathlib import Path
from typing import Any, Optional, Type, NamedTuple, Callable, Iterable
from collections import namedtuple
from itertools import starmap, product

import geopandas
from geopandas import GeoDataFrame
from sqlalchemy import text, delete, true

import geoservice
from geoservice.logging import logger_indent
from geoservice.model import db
from geoservice.model.geoobject import Geoobject
from geoservice.utils.minio import MinioHelper, MinioConfig

import datetime


def named_product(**items) -> Iterable[NamedTuple]:
    return starmap(namedtuple('Product', items.keys()), product(*items.values()))


class DataSourceBase(ABC):

    QUALITIES: dict[list[Any]] = {
        'simplification_level': list(range(11)),
        'adm_level': ['adm1','adm0']
    }
    LOCAL_STORAGE_PATH: Optional[Path] = None
    ENGINE: str = "pyogrio"
    MODEL: Optional[Type[Geoobject]] = None
    CUSTOM_FLOW: bool = False
    logger = logging.getLogger('geoservice.etl')

    # ---------------------------------------
    @classmethod
    def _model(cls, qualities: NamedTuple) -> Type[Geoobject]:
        return cls.MODEL

    @classmethod
    def _local_storage_path(cls, qualities: NamedTuple) -> Path:
        return cls.LOCAL_STORAGE_PATH

    @classmethod
    def _remote_storage_path(cls, qualities: NamedTuple) -> str:
        return cls._cfg_remote_storage_path()

    @classmethod
    def _layer(cls, qualities: NamedTuple) -> str:
        return None
    # ---------------------------------------
    @classmethod
    def _cfg_lookup(cls, s):
        return (
            geoservice.app.config['ETL_REMOTE_SOURCES'].get(cls.__name__, {s: None}).get(s, None)
            or geoservice.app.config.get(f'MINIO_DEFAULT_{s}', None)
        )

    @classmethod
    def _cfg_secret_lookup(cls, s):
        return (
            geoservice.app.config['ETL_REMOTE_SOURCES_SECRETS'].get(cls.__name__, {s: None}).get(s, None)
            or geoservice.app.config[f'MINIO_DEFAULT_{s}']
        )

    _cfg_remote_storage_path = partialmethod(_cfg_lookup, 'PATH')
    _cfg_remote_storage_path_lut = partialmethod(_cfg_lookup, 'LUT')
    _cfg_remote_bucket = partialmethod(_cfg_lookup, 'BUCKET')
    _cfg_remote_server = partialmethod(_cfg_lookup, 'SERVER')
    _cfg_remote_access_key = partialmethod(_cfg_lookup, 'ACCESS_KEY')
    _cfg_remote_database_user = partialmethod(_cfg_lookup, 'DATABASE_USER')
    _cfg_remote_database_name = partialmethod(_cfg_lookup, 'DATABASE_NAME')
    _cfg_remote_secret_key = partialmethod(_cfg_secret_lookup, 'SECRET_KEY')
    _cfg_remote_database_password = partialmethod(_cfg_secret_lookup, 'DATABASE_PASSWORD')

    # ---------------------------------------

    @classmethod
    def _extract(
        cls,
        qualities: Optional[NamedTuple] = None,
        local_storage_path: Path = None,
        load_function: Callable[[Path, type, Optional[NamedTuple]], Any] = (
            lambda path, cls, qualities: geopandas.read_file(
                path,
                driver=cls.ENGINE,
                layer=cls._layer(qualities))
        ),
        store_function: Callable[[Any, Any, type, Optional[NamedTuple]], None] = (
            lambda data_obj, source, cls, qualities: data_obj.to_file(
                filename=str(source),
                engine=cls.ENGINE,
                layer=cls._layer(qualities)
            )
        ),
        fetch_mode: bool = False
    ) -> Any:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            use_local_runtime = geoservice.app.config.get('LOCAL_RUNTIME')
            if use_local_runtime:
                return cls._extract_local(
                    qualities=qualities,
                    local_storage_path=local_storage_path,
                    load_function=load_function,
                    store_function=store_function,
                    fetch_mode=fetch_mode
                )
            else:
                return cls._extract_remote(
                    qualities=qualities,
                    load_function=load_function,
                )

    @classmethod
    def _extract_local(
        cls,
        store_function: Callable,
        load_function: Callable,
        fetch_mode: bool,
        qualities: Optional[NamedTuple] = None,
        local_storage_path: Path = None,
    ) -> Any:
        local_storage_path = local_storage_path or cls.LOCAL_STORAGE_PATH or cls._local_storage_path(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        if not local_storage_path.exists():
            if geoservice.app.config.get('ETL_PULL_MISSING_FILES_FOR_LOCAL_RUNTIME'):
                cls.logger.info(f'{local_storage_path} not found')
                data_obj = cls._extract_remote(
                    qualities=qualities,
                    load_function=load_function
                )
                store_function(data_obj, local_storage_path, cls, qualities)
                return data_obj
            else:
                cls.logger.error(f'{local_storage_path} not found. Aborting')
        else:
            cls.logger.info(f'Using {local_storage_path}')
        # - - - - - - - - - - - - - - - - - - - -
        if fetch_mode:
            return
        # - - - - - - - - - - - - - - - - - - - -
        return load_function(local_storage_path, cls, qualities)

    @classmethod
    def _extract_remote(
        cls,
        qualities: Optional[NamedTuple] = None,
        load_function: Callable = None,
    ) -> Any:
        minio_storage_path = cls._cfg_remote_storage_path() or cls._remote_storage_path(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        cls.logger.info('Pulling from Minio...')
        print(minio_storage_path)
        with MinioHelper(
            objectname=minio_storage_path,
            config=MinioConfig(
                server=cls._cfg_remote_server(),
                access_key=cls._cfg_remote_access_key(),
                secret_key=cls._cfg_remote_secret_key(),
                bucket=cls._cfg_remote_bucket()
            )
        ) as minio:
            return load_function(minio.response, cls, qualities)

    @classmethod
    def _transform(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> GeoDataFrame:
        return gdf

    @classmethod
    def _persist(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> None:
        model = cls.MODEL or cls._model(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        db.session.execute(
            delete(model).where(
                model.source == cls.__name__,
                model.geometry_level == qualities._asdict().get('simplification_level', 0)
                if hasattr(model, 'geometry_level') else true()
            )
        )
        # - - - - - - - - - - - - - - - - - - - -
        db.session.add_all(list(map(lambda row: model(**row[1].to_dict()), gdf.iterrows())))
        db.session.commit()

    @classmethod
    def _custom_etl_flow(cls, qualities: Optional[NamedTuple] = None):
        return NotImplementedError

    @classmethod
    def _custom_extract_flow(cls, qualities: Optional[NamedTuple] = None):
        return NotImplementedError
    # ---------------------------------------

    @classmethod
    def _execute_update(
        cls,
        quality_allocation: Optional[NamedTuple] = None,
        quality_restrictions: Optional[dict[str, str]] = None,
    ):
        if quality_restrictions and quality_allocation and not all(
            [
                quality_restrictions[key] == str(quality_allocation._asdict()[key])
                for key in set(quality_restrictions.keys()).intersection(set(quality_allocation._asdict().keys()))
            ]
        ):
            return
        # - - - - - - - - - - - - - - - - - - - -
        if quality_allocation:
            cls.logger.info(f'Now handling case {quality_allocation._asdict()}')
        # - - - - - - - - - - - - - - - - - - - -
        if cls.CUSTOM_FLOW:
            return cls._custom_etl_flow(qualities=quality_allocation)
        # - - - - - - - - - - - - - - - - - - - -
        df = cls._extract(quality_allocation)
        cls.logger.info('Transforming...')
        df = cls._transform(df, quality_allocation)
        cls.logger.info('Persisting...')
        cls._persist(df, quality_allocation)

    @classmethod
    def _execute_fetch_only(
        cls,
        quality_allocation: Optional[NamedTuple] = None,
    ):
        if quality_allocation:
            cls.logger.info(f'Now handling case {quality_allocation._asdict()}')
        # - - - - - - - - - - - - - - - - - - - -
        if cls.CUSTOM_FLOW:
            return cls._custom_extract_flow(qualities=quality_allocation)
        # - - - - - - - - - - - - - - - - - - - -
        cls._extract(quality_allocation, fetch_mode=True)

    @classmethod
    def execute_fetch_only(cls, datasource_restrictions: Optional[list[str]] = None):
        if datasource_restrictions and cls.__name__.replace('DataSource', '').lower() not in datasource_restrictions:
            cls.logger.debug(f'Skipping fetch for {cls.__name__}')
            return
        # - - - - - - - - - - - - - - - - - - - -
        cls.logger.info(f'Fetching for {cls.__name__}...')
        try:
            with logger_indent():
                if not cls.QUALITIES:
                    cls._execute_fetch_only()
                else:
                    for quality_allocation in named_product(**cls.QUALITIES):
                        cls._execute_fetch_only(quality_allocation=quality_allocation)
        except Exception as e:
            with logger_indent():
                cls.logger.exception(e)
                cls.logger.error(str(e))
            cls.logger.error(f'Fetching for {cls.__name__} failed')
        else:
            cls.logger.info(f'Fetching for {cls.__name__} complete')

    @classmethod
    def execute_update(
        cls,
        quality_restrictions: Optional[dict[str, str]] = None,
        datasource_restrictions: Optional[list[str]] = None
    ):
        if datasource_restrictions and cls.__name__.replace('DataSource', '').lower() not in datasource_restrictions:
            cls.logger.debug(f'Skipping update for {cls.__name__}')
            return
        # - - - - - - - - - - - - - - - - - - - -
        cls.logger.info(f'Running update for {cls.__name__}...')
        try:
            with logger_indent():
                if not cls.QUALITIES:
                    cls._execute_update(quality_restrictions=quality_restrictions)
                else:
                    for quality_allocation in named_product(**cls.QUALITIES):
                        cls._execute_update(
                            quality_allocation=quality_allocation,
                            quality_restrictions=quality_restrictions
                        )
        except Exception as e:
            with logger_indent():
                cls.logger.exception(e)
                cls.logger.error(str(e))
            cls.logger.error(f'Running update for {cls.__name__} failed')
        else:
            cls.logger.info(f'Running update for {cls.__name__} complete')

    @classmethod
    def _sql_update_metadatastate(cls, source:str, qualities: Optional[NamedTuple] = None):
        """
        This function updates the adaption date of the metadata relation 
        """
        currentdatetime = datetime.datetime.now()
        db.session.execute(text("""UPDATE metadata
                                SET "adaptionDate" = :adaptionDate
                                WHERE source = :source;"""),
                                {'source':source,
                                 'adaptionDate':currentdatetime})
        db.session.commit()

    @classmethod
    def _sql_update_bbox(cls, source:str, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None):
        """
        This function updates the bbox of the metadata relation 
        """
        bbox = f"""{'{'} {gdf.geometry.total_bounds[0]}, {gdf.geometry.total_bounds[1]}, {gdf.geometry.total_bounds[2]}, {gdf.geometry.total_bounds[3]}{'}'}"""
        
        db.session.execute(text("""UPDATE metadata
                                SET "geoBox" = :geoBox
                                WHERE source = :source;"""),
                                {'source':source,
                                 'geoBox':bbox})
        db.session.commit()
    
    @classmethod
    def _sql_update_crs(cls, source:str, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None):
        """
        This function updates the crs of the metadata relation 
        """
        crs = gdf.crs
        
        db.session.execute(text("""UPDATE metadata
                                SET "crs" = :crs
                                WHERE source = :source;"""),
                                {'source':source,
                                 'crs':crs})
        db.session.commit()
