# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from enum import Enum

from marshmallow import Schema, fields, validates_schema, ValidationError
from sqlalchemy import select
from geoalchemy2.elements import WKTElement
import geopandas
from pandas import concat
from shapely.geometry import box
from shapely.wkt import dumps

from ..model import db
from ..model.geoobject import Adm0, Adm1, Consulates, Population, PopulatedPlaces, LinkTable


class GeoobjectArgsSchema(Schema):
    filter = fields.Str(load_default='')
    limit = fields.Int(min=0, load_default=None)


class Adm0ResponseSchema(Schema):
    items = fields.List(fields.Nested('Adm0Schema'))


class Adm0Schema(Schema):
    adm0_code = fields.Str()
    name = fields.Str()
    geometry_level = fields.Int()


class Adm1ResponseSchema(Schema):
    items = fields.List(fields.Nested('Adm1Schema'))


class Adm1Schema(Schema):
    adm0_code = fields.Str()
    adm1_code = fields.Str()
    name = fields.Str()
    geometry_level = fields.Int()


class AdmLevel(Enum):
    ADM0 = "ADM0"
    ADM1 = "ADM1"


class GeoServiceArgs(Schema):
    filter_aerial_code = fields.List(fields.Str())
    filter_aerial_level = fields.Enum(AdmLevel)
    filter_boundingbox_southwest_lat = fields.Float(load_default = -90)
    filter_boundingbox_southwest_lng = fields.Float(load_default = -180)
    filter_boundingbox_northeast_lat = fields.Float(load_default = 90)
    filter_boundingbox_northeast_lng = fields.Float(load_default = 180)
    zoom_level = fields.Int()
    source = fields.Str()
    feature_geometries = fields.Boolean()
    feature_population = fields.Boolean()
    feature_consulates = fields.Boolean()
    feature_cities = fields.Boolean()

    @validates_schema
    def validate_method(self, args, **kwargs):
        if ((args.get('filter_aerial_level', AdmLevel("ADM0")).value in ['ADM1']) and 
            args.get('feature_population', False)):
            raise ValidationError(f"""feature_population only available for ADM0""")

    @classmethod
    def _feature_geometry(cls, show, query, bbox):
        return query

    @classmethod
    def _get_aerial_codes(cls, level, source="gadm", codes=[]):
        if not codes:
            return []

        return [x[0] for x in db.session.query(
                LinkTable.link_to_code
            ).where(
                LinkTable.link_to_aerial_level == level,
                LinkTable.link_to_source == source,
                LinkTable.iso_3166_1_a3.in_(codes),
            ).all()
        ]

    @classmethod
    def fetch(cls, query_arguments):
        aerial_codes = query_arguments.get('filter_aerial_code', [])
        source = {
            "naturalearth": "naturalearth"
        }.get(query_arguments.get("source"), "gadm")
        bbox = WKTElement(dumps(box(
            query_arguments.get('filter_boundingbox_southwest_lng', -180),
            query_arguments.get('filter_boundingbox_southwest_lat', -90),
            query_arguments.get('filter_boundingbox_northeast_lng', 180),
            query_arguments.get('filter_boundingbox_northeast_lat', 90),
        )), srid=4326)
        simplification_level = 10 - int(query_arguments.get('zoom_level', 2) - 1) // 1.1

        if simplification_level < 0: simplification_level = 0
        if simplification_level > 10: simplification_level = 10

        aerial_level = query_arguments.get('filter_aerial_level', [])

        gpds = [geopandas.GeoDataFrame()]
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Geometries
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if query_arguments.get('feature_geometries', True):
            geom_aerial_codes = cls._get_aerial_codes("adm0", source, aerial_codes)
            if query_arguments.get('filter_aerial_level', AdmLevel("ADM0")).value == 'ADM0':
                geometries = select(
                    Adm0.adm0_code,
                    db.func.ST_Intersection(Adm0.geometry, bbox).label('geometry')
                ).filter(*[
                    *([Adm0.adm0_code.in_(geom_aerial_codes)] if len(geom_aerial_codes) > 0 else []),
                    Adm0.geometry_level == simplification_level,
                    Adm0.source == source,
                    db.func.ST_Intersects(Adm0.geometry, bbox)
                ])
                if query_arguments.get('feature_population', False):
                    population_aerial_codes = cls._get_aerial_codes("adm0", "population", aerial_codes)
                    population = select(
                        Population.adm0_code.label("adm0_code"),
                        Population.value.label("population"),
                    ).filter(*[
                        *([Population.adm0_code.in_(population_aerial_codes)] if len(population_aerial_codes) > 0 else []),
                        Population.year == 2021
                    ]).subquery()
                    geometries = geometries.join(population, population.c.adm0_code == Adm0.adm0_code)
                    geometries = geometries.add_columns(
                        population.c.population.label("population")
                    )
            else:
                geometries = select(
                    Adm1.adm0_code,
                    Adm1.adm1_code,
                    db.func.ST_Intersection(Adm1.geometry, bbox).label('geometry')
                ).filter(*[
                    *([Adm1.adm0_code.in_(geom_aerial_codes)] if len(geom_aerial_codes) > 0 else []),
                    Adm1.geometry_level == simplification_level,
                    Adm1.source == source,
                    db.func.ST_Intersects(Adm1.geometry, bbox)
                ])
            g_gpd = geopandas.read_postgis(geometries, con=db.engine, geom_col='geometry')
            gpds.append(g_gpd)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Consulates
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if query_arguments.get('feature_consulates', False):
            consulates = select(
                Consulates.adm0_code,
                db.func.json_build_object(
                    "sovereign_code", Consulates.sovereign_code,
                    "code", Consulates.consulate_code,
                    "name", Consulates.name_de,
                    "url", Consulates.url
                ).label("consulate"),
                db.func.ST_Intersection(Consulates.geometry, bbox).label("geometry")
            ).filter(*[
                *([Consulates.adm0_code.in_(aerial_codes)] if len(aerial_codes) > 0 else []),
                db.func.ST_Intersects(Consulates.geometry, bbox)
            ])
            c_gpd = geopandas.read_postgis(consulates, con=db.engine, geom_col='geometry')
            gpds.append(c_gpd)

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # Populated places
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if query_arguments.get('feature_cities', True):
            if query_arguments.get('filter_aerial_level', None):
                populated_places = select(
                    PopulatedPlaces.adm0_code,
                    PopulatedPlaces.capital_level,
                    PopulatedPlaces.nameascii,
                    PopulatedPlaces.name_de,
                    PopulatedPlaces.name_en,
                    PopulatedPlaces.name_fr,
                    PopulatedPlaces.population,
                    db.func.ST_Intersection(PopulatedPlaces.geometry, bbox).label('geometry')
                ).filter(*[
                    *([PopulatedPlaces.adm0_code.in_(aerial_codes)] if len(aerial_codes) > 0 else []),
                    PopulatedPlaces.capital_level == aerial_level.value.lower(),
                    db.func.ST_Intersects(PopulatedPlaces.geometry, bbox)
                ])
            else:
                populated_places = select(
                    PopulatedPlaces.adm0_code,
                    PopulatedPlaces.capital_level,
                    PopulatedPlaces.nameascii,
                    PopulatedPlaces.name_de,
                    PopulatedPlaces.name_en,
                    PopulatedPlaces.name_fr,
                    PopulatedPlaces.population,
                    db.func.ST_Intersection(PopulatedPlaces.geometry, bbox).label('geometry')
                ).filter(*[
                    *([PopulatedPlaces.adm0_code.in_(aerial_codes)] if len(aerial_codes) > 0 else []),
                    db.func.ST_Intersects(PopulatedPlaces.geometry, bbox)
                ])
            p_gpd = geopandas.read_postgis(populated_places, con=db.engine, geom_col='geometry')
            gpds.append(p_gpd)

        gpd = concat(gpds, ignore_index=True)

        return gpd


class Weight(Schema):
    code = fields.Str()
    value = fields.Float()


class GeoServiceImageArgs(GeoServiceArgs):
    filter_image_weights = fields.List(fields.Nested(Weight))
