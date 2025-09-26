# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from pathlib import Path
from typing import NamedTuple, Optional, Type

from geopandas import GeoDataFrame
from sqlalchemy import text, delete, true

from geoservice.constants import RESOURCES_PATH
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.model.geoobject import Adm1, Geoobject, Adm0
from geoservice.model import db


class DataSourceNaturalearth(DataSourceBase):

    @classmethod
    def _check_adm_data(cls, source:str, adm_level:str, simplification_level:int) -> bool:
        """
        This function checks if any elements in the source table have the selected adm level and simplification level
        """
        if simplification_level  not in range(0,11):
            raise
        if adm_level not in ['adm0', 'adm1']:
            raise

        ret_val = db.session.execute(text(
            f"""
            SELECT name
            FROM {adm_level}
            WHERE geometry_level = :simplification_level AND source = :source; 
            """),
            {'simplification_level':simplification_level,
            'source':source})
        return (len(ret_val.all()) > 0)


    @classmethod
    def _local_storage_path(cls, qualities: NamedTuple) -> Path:
        return RESOURCES_PATH / 'naturalearth' / f'{getattr(qualities, "adm_level")}.gpkg'

    @classmethod
    def _layer(cls, qualities: NamedTuple) -> str:
        return {
            "adm0": "ne_10m_admin_0_countries",
            "adm1": "ne_10m_admin_1_states_provinces",
        }[qualities.adm_level]

    @classmethod
    def _model(cls, qualities: NamedTuple) -> Type[Geoobject]:
        return {
            'adm0': Adm0,
            'adm1': Adm1
        }[getattr(qualities, 'adm_level')]

    @classmethod
    def _extract(cls, qualities: Optional[NamedTuple] = None, fetch_mode: bool = False, **kwargs) -> GeoDataFrame:
        if not cls._check_adm_data('naturalearth', 'adm1', 0) or ((qualities.simplification_level == 0) and (qualities.adm_level == 'adm1')):
            gdf = super()._extract(qualities._replace(adm_level='adm0'), fetch_mode=fetch_mode, **kwargs)
            # - - - - - - - - - - - - - - - - - - - -

            gdf_adm1 = super()._extract(qualities._replace(adm_level='adm1'), fetch_mode=fetch_mode, **kwargs)
            if fetch_mode:
                return
            gdf = gdf_adm1.merge(
                gdf[['GU_A3', 'NAME']].rename(columns={"NAME": "name_0", "GU_A3": "GU_A3_0"}),
                left_on="adm0_a3",
                right_on="GU_A3_0",
                how="left"
                )
            # - - - - - - - - - - - - - - - - - - - -
            return gdf

    @classmethod
    def _transform(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> GeoDataFrame:
        if not cls._check_adm_data('naturalearth', 'adm1', 0) or ((qualities.simplification_level == 0) and (qualities.adm_level == 'adm1')):
            columns_to_keep_by_adm_level = {
                "adm0": ["name_0", "adm0_a3", "adm1_code", "geometry"],
                "adm1": ["name", "adm0_a3", "adm1_code", "geometry","name_0"],
            }
            gdf.drop(set(gdf.columns) - set(columns_to_keep_by_adm_level['adm1']), axis=1, inplace=True)
            # - - - - - - - - - - - - - - - - - - - -
            gdf.rename(
                columns={"adm0_a3": "adm0_code",
                        "name_0": "adm0_name"},
                inplace=True,
            )
            # - - - - - - - - - - - - - - - - - - - -
            gdf["geometry_level"] = 0
            gdf["source"] = "naturalearth"
            # - - - - - - - - - - - - - - - - - - - -
            return gdf
    
    @classmethod
    def _sql_replace_adm1_1to10(cls, source:str, simp_fact:float, qualities: Optional[NamedTuple] = None) -> bool:
        """
        This function deletes the old adm1 entries for the given simplification_level and creates a simplification based on simplification level 0
        """
        if qualities.simplification_level  in range(1,11):
            db.session.execute(text("""DELETE FROM adm1 
                                        WHERE source = :source 
                                            AND geometry_level = :simplification_level;"""),
                                    {'source':source,
                                     'simplification_level':qualities.simplification_level})
            db.session.execute(text(f"""
                    INSERT INTO adm1(adm0_code, adm1_code, name, geometry_level, source, geometry, adm0_name )
                    WITH simplif AS (
                        SELECT adm0_code, 
                            adm1_code, 
                            name, 
                            {qualities.simplification_level} as geometry_level,
                            source, 
                            ST_CoverageSimplify(geometry, :simp_fact) OVER () AS geometry, 
                            adm0_name
                        FROM adm1
                        WHERE geometry_level = 0 AND source = :source
                    )
                    SELECT adm0_code, adm1_code, name, geometry_level, source, ST_MakeValid(geometry) AS geometry, adm0_name
                    FROM simplif
                                    """),
                                    {'source':source,
                                     'simp_fact':simp_fact})
            db.session.commit()

    @classmethod
    def _sql_replace_adm0_0(cls, source:str, simp_fact:float, qualities: Optional[NamedTuple] = None) -> bool:
        """
        This function deletes the old adm0 entries for simplification_level 0 and creates a union based on adm level 1
        """
        if qualities.simplification_level == 0: 
            db.session.execute(text("""DELETE FROM adm0 
                                        WHERE source = :source 
                                            AND geometry_level = :simplification_level;"""),
                               {'source':source,
                                'simplification_level':qualities.simplification_level}) 
            db.session.execute(text(f"""
                    INSERT INTO adm0(adm0_code, name, geometry_level, source, geometry )
                    WITH simplif AS (
                        SELECT adm0_code, 
                            adm0_name as name, 
                            {qualities.simplification_level} as geometry_level, 
                            source, 
                            geometry
                        FROM adm1
                        WHERE geometry_level = 0 AND source = :source
                    )
                    SELECT adm0_code, name, geometry_level, source, ST_Union(ST_MakeValid(geometry)) AS geometry 
                    FROM simplif 
                    GROUP BY adm0_code, name, geometry_level, source
                                    """),
                                    {'source':source,
                                     'simp_fact':simp_fact})
            db.session.commit()
    
    @classmethod
    def _sql_replace_adm0_1to10(cls, source:str, simp_fact:float, qualities: Optional[NamedTuple] = None) -> bool:
        """
        This function deletes the old adm0 entries for the given simplification_level and creates a new simplification 
        """
        if qualities.simplification_level in range(1,11): 
                db.session.execute(text("""DELETE FROM adm0 
                                            WHERE source = :source 
                                                AND geometry_level = :simplification_level;"""),
                                {'source':source,
                                 'simplification_level':qualities.simplification_level})     
                if (cls._check_adm_data(source, 'adm1', qualities.simplification_level)):
                    db.session.execute(text(f"""
                    INSERT INTO adm0(adm0_code, name, geometry_level, source, geometry )
                    WITH simplif AS (
                        SELECT adm0_code, 
                            adm0_name as name, 
                            {qualities.simplification_level} as geometry_level, 
                            source, 
                            geometry 
                        FROM adm1 
                        WHERE geometry_level = :simplification_level AND source = :source
                    )
                    SELECT adm0_code, name, geometry_level, source, ST_Union(geometry) AS geometry 
                    FROM simplif
                    GROUP BY adm0_code, name, geometry_level, source
                                    """),
                                    {'source':source,
                                    'simp_fact':simp_fact,
                                    'simplification_level':qualities.simplification_level})
                else:
                    db.session.execute(text(f"""
                    INSERT INTO adm0(adm0_code, name, geometry_level, source, geometry )
                    WITH simplif AS (
                        SELECT adm0_code, 
                            adm0_name as name, 
                            {qualities.simplification_level} as geometry_level, 
                            source, 
                            ST_CoverageSimplify(geometry, :simp_fact) OVER () AS geometry 
                        FROM adm1 
                        WHERE geometry_level = 0 AND source = :source
                    )
                    SELECT adm0_code, name, geometry_level, source, ST_Union(ST_MakeValid(geometry)) AS geometry 
                    FROM simplif 
                    GROUP BY adm0_code, name, geometry_level, source
                                    """),
                                    {'source':source,
                                    'simp_fact':simp_fact})
                db.session.commit()


    @classmethod
    def _persist(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> None:
        source='naturalearth'

        if not cls._check_adm_data('naturalearth', 'adm1', 0) or ((qualities.simplification_level == 0) and (qualities.adm_level == 'adm1')):
                if not cls._check_adm_data('naturalearth', 'adm1', 0):
                    cls.logger.info("adm_level adm1 with simplification_level 0 required but not in database ...")
                elif  (qualities.simplification_level == 0) and (qualities.adm_level == 'adm1'):
                    cls.logger.info("adm_level adm1 with simplification_level 0 gets reloaded in database ...")

                model = cls._model(qualities._replace(adm_level='adm1', simplification_level=0))
                # - - - - - - - - - - - - - - - - - - - -
                db.session.execute(
                    delete(model).where(
                        model.source == 'naturalearth',
                        model.geometry_level == 0
                        if hasattr(model, 'geometry_level') else true()
                    )
                )
                # - - - - - - - - - - - - - - - - - - - -
                db.session.add_all(list(map(lambda row: model(**row[1].to_dict()), gdf.iterrows())))
                db.session.commit()
                cls.logger.info("adm_level adm1 with simplification_level 0 loaded in database ...")
                cls._sql_update_bbox(source, gdf, qualities)
                cls._sql_update_crs(source, gdf, qualities)

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

        if qualities.adm_level == 'adm1':
            if qualities.simplification_level  in range(1,11):
                cls._sql_replace_adm1_1to10(source, simp_fact, qualities)
        
        if qualities.adm_level == 'adm0':
            if qualities.simplification_level == 0: 
                cls._sql_replace_adm0_0(source, simp_fact, qualities)
            if qualities.simplification_level in range(1,11): 
                cls._sql_replace_adm0_1to10(source, simp_fact, qualities)
        
        cls._sql_update_metadatastate(source, qualities)
        

        
    

