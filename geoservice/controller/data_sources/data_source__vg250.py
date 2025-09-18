from pathlib import Path
from typing import NamedTuple, Optional, Type, Callable, Any

from geopandas import GeoDataFrame, list_layers
from sqlalchemy import text, delete, true, create_engine

import geoservice
from geoservice.constants import RESOURCES_PATH
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.model.geoobject import VG250Attributes, Geoobject, VG250
from geoservice.model import db
from geoservice.exceptions import GeoserviceInputException


class DataSourceVG250(DataSourceBase):

    QUALITIES: dict[list[Any]] = {
        'simplification_level': list(range(11)),
        'adm_level': ["gemeinde", "land", "regierungsbezirk", "kreis", "verwaltungsgemeinschaft", "nuts1", "nuts2", "nuts3"]
    }

    COLUMNS: dict[list[str]] = {
        "land": ['ARS_L', 'GEN_L', 'geometry'],
        "regierungsbezirk": ['ARS_R', 'GEN_R', 'geometry'],
        "kreis": ['ARS_K', 'GEN_K', 'geometry'],
        "verwaltungsgemeinschaft": ['ARS_V', 'GEN_V', 'geometry'],
        "gemeinde": ['ARS_G', 'GEN_G', 'geometry'],
        "nuts1": ['NUTS1_CODE', 'NUTS1_NAME', 'geometry'],
        "nuts2": ['NUTS2_CODE', 'NUTS2_NAME', 'geometry'],
        "nuts3": ['NUTS3_CODE', 'NUTS3_NAME', 'geometry'],
        "attributes": ['ARS_G', 'GEN_G',
                       'ARS_V', 'GEN_V',
                       'ARS_K', 'GEN_K',
                       'ARS_R', 'GEN_R',
                       'ARS_L', 'GEN_L',
                       'NUTS1_CODE', 'NUTS1_NAME',
                       'NUTS2_CODE', 'NUTS2_NAME',
                       'NUTS3_CODE', 'NUTS3_NAME',
                       'EWZ'],
    }

    @classmethod
    def _check_agg_data(cls, agg_level: str, simplification_level: int) -> bool:
        """
        This function checks if any elements in the source table have the selected adm level and simplification level
        """
        if simplification_level not in range(0, 11):
            raise GeoserviceInputException(
                "Selected simplification_level not between 0 and 10")
        if agg_level not in ["land", "regierungsbezirk", "kreis", "verwaltungsgemeinschaft", "gemeinde", "nuts1", "nuts2", "nuts3"]:
            raise GeoserviceInputException(
                "Selected agg_level not in defined list")

        ret_val = db.session.execute(text(
            f"""
            SELECT name
            FROM vg250
            WHERE geometry_level = :simplification_level AND agg_level = :agg_level;
            """),
            {'simplification_level': simplification_level,
             'agg_level': agg_level})
        return (len(ret_val.all()) > 0)

    @classmethod
    def _local_storage_path(cls, qualities: NamedTuple) -> Path:
        return RESOURCES_PATH / 'vg' / 'DE_VG250.gpkg'

    @classmethod
    def _layer(cls, qualities: NamedTuple) -> str:
        return {
            "vg250_gem": "vg250_gem",
            "vgtb_vz_gem": "vgtb_vz_gem",
        }[qualities.adm_level]

    @classmethod
    def _model(cls, qualities: NamedTuple) -> Type[Geoobject]:
        return {
            'vg250': VG250,
            'vg250_attributes': VG250Attributes
        }[getattr(qualities, 'adm_level')]

    @classmethod
    def _extract_local(
        cls,
        store_function: Callable,
        load_function: Callable,
        fetch_mode: bool,
        qualities: Optional[NamedTuple] = None,
        local_storage_path: Path = None,
    ) -> Any:
        local_storage_path = local_storage_path or cls.LOCAL_STORAGE_PATH or cls._local_storage_path(
            qualities)
        # - - - - - - - - - - - - - - - - - - - -
        if (not local_storage_path.exists()) or (cls._layer(qualities) not in list_layers(local_storage_path).name.to_list()):
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
    def _store_pandas(cls, data_obj, source, qualities):
        """
        This function stores a non geographic pandas dataset which is imported from the original geopackage (a sqlite file) 
        in the designated source (also a geopackage/sqlite) without using geopandas
        """
        engine = create_engine(f'sqlite:///{source}', echo=False)
        conn = engine.connect()
        data_obj.to_sql(cls._layer(qualities), conn,
                        if_exists='replace', index=False)
        conn.close()

    @classmethod
    def _extract(cls, qualities: Optional[NamedTuple] = None, fetch_mode: bool = False, **kwargs) -> GeoDataFrame:
        geo = super()._extract(qualities._replace(adm_level='vg250_gem'),
                               fetch_mode=fetch_mode, **kwargs).to_crs(4326)
        info = super()._extract(qualities._replace(
            adm_level='vgtb_vz_gem'), fetch_mode=fetch_mode, **kwargs,
            store_function=(
            lambda data_obj, source, cls, qualities: cls._store_pandas(
                data_obj, source, qualities)
        ))
        layer = geo.merge(info, how='left', left_on='ARS', right_on='ARS_G')
        return layer

    @classmethod
    def _transform_attributes(cls, dataframe: GeoDataFrame) -> GeoDataFrame:
        """
        This function transforms the vg250 attributes
        """
        dataframe.drop(set(dataframe.columns) -
                       set(cls.COLUMNS["attributes"]), axis=1, inplace=True)
        dataframe.rename(
            columns={"ARS_L": "arsl",
                     "ARS_R": "arsr",
                     "ARS_K": "arsk",
                     "ARS_V": "arsv",
                     "ARS_G": "arsg",
                     "NUTS1_CODE": "nuts1code",
                     "NUTS2_CODE": "nuts2code",
                     "NUTS3_CODE": "nuts3code",
                     "GEN_L": "genl",
                     "GEN_R": "genr",
                     "GEN_K": "genk",
                     "GEN_V": "genv",
                     "GEN_G": "geng",
                     "NUTS1_NAME": "nuts1name",
                     "NUTS2_NAME": "nuts2name",
                     "NUTS3_NAME": "nuts3name",
                     "EWZ": "ewz"},
            inplace=True,
        )
        dataframe["source"] = f"vg250"

        return dataframe

    @classmethod
    def _transform_geofeatures(cls, dataframe: GeoDataFrame, simplification_level: str, agg_level: str) -> GeoDataFrame:
        """
        This function transforms the vg250 geodata
        """
        dataframe.drop(set(dataframe.columns) -
                       set(cls.COLUMNS[agg_level]), axis=1, inplace=True)
        dataframe.rename(
            columns={"ARS_L": "code",
                     "ARS_R": "code",
                     "ARS_K": "code",
                     "ARS_V": "code",
                     "ARS_G": "code",
                     "NUTS1_CODE": "code",
                     "NUTS2_CODE": "code",
                     "NUTS3_CODE": "code",
                     "GEN_L": "name",
                     "GEN_R": "name",
                     "GEN_K": "name",
                     "GEN_V": "name",
                     "GEN_G": "name",
                     "NUTS1_NAME": "name",
                     "NUTS2_NAME": "name",
                     "NUTS3_NAME": "name"},
            inplace=True,
        )
        dataframe["geometry_level"] = simplification_level
        dataframe["agg_level"] = agg_level
        dataframe["source"] = f"vg250_{agg_level}_{simplification_level}"

        return dataframe

    @classmethod
    def _sql_replace_vg250_1to10_gemeinde(cls, simp_fact: float, qualities: Optional[NamedTuple] = None) -> bool:
        """
        This function deletes the old vg250 entries for the given simplification_level and creates a simplification based on simplification level 0
        """
        if qualities.adm_level in ["land", "regierungsbezirk", "kreis", "verwaltungsgemeinschaft", "gemeinde", "nuts1", "nuts2", "nuts3"]:
            if qualities.simplification_level in range(1, 11):
                db.session.execute(text("""DELETE FROM vg250 
                                            WHERE agg_level = :agg_level 
                                                AND geometry_level = :simplification_level;"""),
                                   {'agg_level': qualities.adm_level,
                                    'simplification_level': qualities.simplification_level})
                db.session.execute(text(f"""
                        INSERT INTO vg250(code, name, geometry_level, agg_level, source, geometry )
                        WITH simplif AS (
                            SELECT code, 
                                name, 
                                {qualities.simplification_level} as geometry_level,
                                '{qualities.adm_level}' as agg_level,
                                source, 
                                ST_CoverageSimplify(geometry, :simp_fact) OVER () AS geometry
                            FROM vg250
                            WHERE geometry_level = 0 AND agg_level = :agg_level 
                        )
                        SELECT code, name, geometry_level, agg_level, source, ST_MakeValid(geometry) AS geometry
                        FROM simplif
                                        """),
                    {'agg_level': qualities.adm_level,
                     'simp_fact': simp_fact})
                db.session.commit()

    @classmethod
    def _sql_replace_vg250_1to10(cls, simp_fact: float, qualities: Optional[NamedTuple] = None):
        """
        This function deletes the old vg250 entries for the given simplification_level and creates a simplification based on simplification level 0
        """

        if qualities.adm_level not in ["land", "regierungsbezirk", "kreis", "verwaltungsgemeinschaft", "gemeinde", "nuts1", "nuts2", "nuts3"]:
            raise GeoserviceInputException(
                "Selected agg_level not in defined list")

        codecolumn = {"land": "arsl",
                      "regierungsbezirk": "arsr",
                      "kreis": "arsk",
                      "verwaltungsgemeinschaft": "arsv",
                      "nuts1": "nuts1code",
                      "nuts2": "nuts2code",
                      "nuts3": "nuts3code"
                      }[qualities.adm_level]

        namecolumn = {"land": "genl",
                      "regierungsbezirk": "genr",
                      "kreis": "genk",
                      "verwaltungsgemeinschaft": "genv",
                      "nuts1": "nuts1name",
                      "nuts2": "nuts2name",
                      "nuts3": "nuts3name"
                      }[qualities.adm_level]

        if qualities.simplification_level in range(0, 11):
            sourcename = f"vg250_{qualities.adm_level}_{qualities.simplification_level}"
            db.session.execute(text("""DELETE FROM vg250 
                                            WHERE agg_level = :agg_level 
                                                AND geometry_level = :simplification_level;"""),
                               {'agg_level': qualities.adm_level,
                                'simplification_level': qualities.simplification_level})
            if (cls._check_agg_data('gemeinde', qualities.simplification_level)):
                db.session.execute(text(f"""
                    INSERT INTO vg250(code, name, geometry_level, agg_level, source, geometry )
                        WITH simplif AS (
                            SELECT vg250_attributes.{codecolumn} as code, vg250_attributes.{namecolumn} as name,  vg250.geometry_level, vg250.agg_level, '{sourcename}' as source, vg250.geometry 
                            FROM vg250
                            JOIN vg250_attributes on vg250.code=vg250_attributes.arsg
                            WHERE vg250.agg_level = 'gemeinde' AND vg250.geometry_level = :simplification_level
                        )
                        SELECT code, name, {qualities.simplification_level} as geometry_level, '{qualities.adm_level}' as agg_level, source, ST_Union(ST_MakeValid(geometry)) AS geometry 
                        FROM simplif 
                        GROUP BY name, code, geometry_level, agg_level, source
                                    """),
                                   {'simplification_level': qualities.simplification_level})
            else:
                db.session.execute(text(f"""
                    INSERT INTO vg250(code, name, geometry_level, agg_level, source, geometry )
                        WITH simplif AS (
                            SELECT vg250_attributes.{codecolumn} as code, vg250_attributes.{namecolumn} as name, vg250.geometry_level, vg250.agg_level, '{sourcename}' as source, ST_CoverageSimplify(vg250.geometry, :simp_fact) OVER () AS geometry 
                            FROM vg250
                            JOIN vg250_attributes on vg250.code=vg250_attributes.arsg
                            WHERE vg250.agg_level = 'gemeinde' AND vg250.geometry_level = 0
                        )
                        SELECT code, name, {qualities.simplification_level} as geometry_level, '{qualities.adm_level}' as agg_level, source, ST_Union(ST_MakeValid(geometry)) AS geometry 
                        FROM simplif 
                        GROUP BY name, code, geometry_level, agg_level, source
                                    """),
                                   {'simp_fact': simp_fact})
            db.session.commit()

    @classmethod
    def _persist(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> None:
        # Only reload attributes and adm1 (level 0) when adm1 (level 0) required
        if not cls._check_agg_data('gemeinde', 0) or ((qualities.simplification_level == 0) and (qualities.adm_level == 'gemeinde')):
            # Add attributes
            cls.logger.info(
                "load vg250 attributes in database ...")
            gdf_attribues = gdf.copy()
            gdf_attribues = cls._transform_attributes(gdf_attribues)
            model_attributes = cls._model(qualities._replace(
                adm_level='vg250_attributes', simplification_level=0))
            # - - - - - - - - - - - - - - - - - - - -
            db.session.execute(
                delete(model_attributes).where(
                    model_attributes.source == 'vg250'
                    if hasattr(model_attributes, 'geometry_level') else true()
                )
            )
            # - - - - - - - - - - - - - - - - - - - -
            db.session.add_all(
                list(map(lambda row: model_attributes(**row[1].to_dict()), gdf_attribues.iterrows())))

            # Add geometries
            if not cls._check_agg_data('gemeinde', 0):
                cls.logger.info(
                    "adm_level gemeinde with simplification_level 0 required but not in database ...")
            elif (qualities.simplification_level == 0) and (qualities.adm_level == 'gemeinde'):
                cls.logger.info(
                    "adm_level gemeinde with simplification_level 0 gets reloaded in database ...")

            gdf = cls._transform_geofeatures(
                gdf, qualities.simplification_level, qualities.adm_level)
            model = cls._model(qualities._replace(
                adm_level='vg250', simplification_level=0))
            # - - - - - - - - - - - - - - - - - - - -
            db.session.execute(
                delete(model).where(
                    model.source == 'vg250',
                    model.geometry_level == 0
                    if hasattr(model, 'geometry_level') else true()
                )
            )
            # - - - - - - - - - - - - - - - - - - - -
            db.session.add_all(
                list(map(lambda row: model(**row[1].to_dict()), gdf.iterrows())))
            db.session.commit()
            cls._sql_update_bbox("vg250", gdf, qualities)
            cls._sql_update_crs("vg250", gdf, qualities)
            cls.logger.info(
                "adm_level adm1 with simplification_level 0 and vg250 attributes loaded in database ...")

        # - - - - - - - - - - - - - - - - - - - -
        simp_fact = [
            0.00001,
            0.00005,
            0.0001,
            0.0005,
            0.001,
            0.005,
            0.01,
            0.05,
            0.1,
            0.5,
        ][qualities.simplification_level - 1]

        if qualities.adm_level == "gemeinde":
            if qualities.simplification_level in range(1, 11):
                cls._sql_replace_vg250_1to10_gemeinde(simp_fact, qualities)

        if qualities.adm_level != "gemeinde":
            cls._sql_replace_vg250_1to10(simp_fact, qualities)

        cls._sql_update_metadatastate("vg250", qualities)
