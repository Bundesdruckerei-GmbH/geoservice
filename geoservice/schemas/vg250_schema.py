from marshmallow import Schema, fields, validates_schema, ValidationError

from sqlalchemy import text, bindparam

from geoservice.model.base import db
from geoservice.exceptions import GeoserviceInputException


_levels = {
    "land": 'arsl',
    "regierungsbezirk": 'arsr',
    "kreis": 'arsk',
    "verwaltungsgemeinschaft": 'arsv',
    "gemeinde": 'arsg',
    "nuts1": 'nuts1code',
    "nuts2": 'nuts2code',
    "nuts3": 'nuts3code',
}

_names = {
    "land": 'genl',
    "regierungsbezirk": 'genr',
    "kreis": 'genk',
    "verwaltungsgemeinschaft": 'genv',
    "gemeinde": 'geng',
    "nuts1": 'nuts1name',
    "nuts2": 'nuts2name',
    "nuts3": 'nuts3name',
}

_codes = {
    "land": 'arsl',
    "regierungsbezirk": 'arsr',
    "kreis": 'arsk',
    "verwaltungsgemeinschaft": 'arsv',
    "gemeinde": 'arsg',
    "nuts1": 'nuts1code',
    "nuts2": 'nuts2code',
    "nuts3": 'nuts3code',
}


class VG250ParameterSchema(Schema):
    agg_level = fields.Str(load_default='verwaltungsgemeinschaft')
    zoom_level = fields.Int()
    filter_level = fields.Str()
    filter_names = fields.List(fields.Str())
    filter_codes = fields.List(fields.Str())
    filter_boundingbox_southwest_lat = fields.Float()
    filter_boundingbox_southwest_lng = fields.Float()
    filter_boundingbox_northeast_lat = fields.Float()
    filter_boundingbox_northeast_lng = fields.Float()

    @classmethod
    def _query_no_filters(cls) -> str:
        """
        Create query chunk to select geometries only by geometry level (0-10) and agg level (land, gemeinde, ...)
        """
        return ("""
            WITH selection AS(
                SELECT code, name, geometry_level, agg_level, source, geometry
                FROM vg250
                WHERE geometry_level = :geometry_level AND agg_level = :agg_level
            )
            """)

    @classmethod
    def _query_filter_by_names(cls, agg_sp: str, filt_sp_n: str) -> str:
        """
        Create query chunk to select geometries by geometry level (0-10), agg level (land, gemeinde, ...) and filter names (land = 'Niedersachsen' ...)
        """
        return (f"""
                WITH selection AS(
                    SELECT code,name,geometry_level,agg_level,source,geometry
                    FROM vg250
                    WHERE code IN (
                        SELECT {agg_sp}
                        FROM vg250_attributes
                        WHERE {filt_sp_n} IN :filter_names) AND geometry_level = :geometry_level
                )
                """)

    @classmethod
    def _query_filter_by_codes(cls, agg_sp: str, filt_sp_c: str) -> str:
        """
        Create query chunk to select geometries by geometry level (0-10), agg level (land, gemeinde, ...) and filter codes (land = '03' ...)
        """
        return (f"""
                WITH selection AS(
                    SELECT code,name,geometry_level,agg_level,source,geometry
                    FROM vg250
                    WHERE code IN (
                        SELECT {agg_sp}
                        FROM vg250_attributes
                        WHERE {filt_sp_c} IN :filter_codes) AND geometry_level = :geometry_level
                )
                """)

    @classmethod
    def _query_create_json_output(cls) -> str:
        """
        Create query chunk to create a FeatureCollection json output
        """
        return ("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(ST_AsGeoJSON(selection.*)::json)
                )
            FROM selection;
            """)

    @classmethod
    def _query_clip_bbox_create_json_output(cls) -> str:
        """
        Create query chunk to to select geometries by Bounding Box and create a FeatureCollection json output
        """
        return ("""
            ,
            clipped AS (
                SELECT code, name, geometry_level, agg_level, source, ST_Intersection(geometry, ST_MakeEnvelope(:xmin, :ymin, :xmax,:ymax, :crs)) as geometry
                FROM selection
                WHERE ST_Intersects(geometry, ST_MakeEnvelope(:xmin, :ymin, :xmax,:ymax, :crs))
            )
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(ST_AsGeoJSON(clipped.*)::json)
            )
            FROM clipped;
            """)

    @classmethod
    def _query_execute_filter_names_bbox(cls, query: str, filter_names: str, geometry_level: str, xmin: float, ymin: float, xmax: float, ymax: float, crs: int):
        """
        Execute a query based on filter names (land = 'Niedersachsen' ...), geometry level (0-10) and bounding box 
        """
        return (db.session.execute(
            text(query).bindparams(
                bindparam('filter_names', value=filter_names,
                          expanding=True),
                bindparam('geometry_level', value=geometry_level),
                bindparam('xmin', value=xmin),
                bindparam('ymin', value=ymin),
                bindparam('xmax', value=xmax),
                bindparam('ymax', value=ymax),
                bindparam('crs', value=crs)
            )
        ))

    @classmethod
    def _query_execute_filter_codes_bbox(cls, query: str, filter_codes: str, geometry_level: str, xmin: float, ymin: float, xmax: float, ymax: float, crs: int):
        """
        Execute a query based on filter codes (land = '03' ...), geometry level (0-10) and bounding box 
        """
        return (db.session.execute(
            text(query).bindparams(
                bindparam('filter_codes', value=filter_codes,
                          expanding=True),
                bindparam('geometry_level', value=geometry_level),
                bindparam('xmin', value=xmin),
                bindparam('ymin', value=ymin),
                bindparam('xmax', value=xmax),
                bindparam('ymax', value=ymax),
                bindparam('crs', value=crs)
            )
        ))

    @classmethod
    def _query_execute_filter_names(cls, query: str, filter_names: str, geometry_level: str):
        """
        Execute a query based on filter names (land = 'Niedersachsen' ...) and geometry level (0-10)
        """
        return (db.session.execute(
            text(query).bindparams(
                bindparam('filter_names', value=filter_names,
                          expanding=True),
                bindparam('geometry_level', value=geometry_level)
            )
        ))

    @classmethod
    def _query_execute_filter_codes(cls, query: str, filter_codes: str, geometry_level: str):
        """
        Execute a query based on filter codes (land = '03' ...) and geometry level (0-10)
        """
        return (db.session.execute(
            text(query).bindparams(
                bindparam('filter_codes', value=filter_codes,
                          expanding=True),
                bindparam('geometry_level', value=geometry_level)
            )
        ))

    @classmethod
    def _query_execute_filter_bbox(cls, query: str, agg_level: str, geometry_level: str, xmin: float, ymin: float, xmax: float, ymax: float, crs: int):
        """
        Execute a query based on geometry level (0-10), agg level (land, gemeinde, ...) and bounding box
        """
        return (db.session.execute(
                text(query).bindparams(
                    bindparam('agg_level', value=agg_level),
                    bindparam('geometry_level', value=geometry_level),
                    bindparam('xmin', value=xmin),
                    bindparam('ymin', value=ymin),
                    bindparam('xmax', value=xmax),
                    bindparam('ymax', value=ymax),
                    bindparam('crs', value=crs)
                )
                ))

    @classmethod
    def _query_execute_no_filter(cls, query: str, agg_level: str, geometry_level: str):
        """
        Execute a query based on geometry level (0-10) and agg level (land, gemeinde, ...)
        """
        return (db.session.execute(
                text(query).bindparams(
                    bindparam('geometry_level', value=geometry_level),
                    bindparam('agg_level', value=agg_level)
                )
                ))

    @classmethod
    def _get_vg250(cls, agg_level="", geometry_level=0, filter_level="", filter_names=[""], filter_codes=[""], xmin=0, ymin=0, xmax=0, ymax=0, crs=4326) -> list:
        """
        Query vg250 and vg250_attributes to return geojson file
        """
        valid_levels = ['land', 'regierungsbezirk', 'kreis',
                        'verwaltungsgemeinschaft', 'gemeinde', 'nuts1', 'nuts2', 'nuts3']

        condition_no_filter_names_codes = (
            len(filter_names) == 0) and (len(filter_codes) == 0)
        if condition_no_filter_names_codes:
            filter_level = ""
        condition_no_filters = filter_level in [""]
        condition_filters_selected = filter_level not in [""]
        condition_filter_names_available = len(filter_names) > 0
        condition_no_bbox = sum([xmin, ymin, xmax, ymax]) == 0
        condition_bbox_selected = sum([xmin, ymin, xmax, ymax]) != 0

        if agg_level not in valid_levels:
            raise GeoserviceInputException(
                "Selected agg_level not in defined list")
        else:
            agg_sp = _levels[agg_level]

        if condition_no_filters:
            selection = cls._query_no_filters()
        else:
            if filter_level not in valid_levels:
                raise GeoserviceInputException(
                    "Selected filter_level not in defined list")
            else:
                filt_sp_n = _names[filter_level]
                filt_sp_c = _codes[filter_level]

            if condition_filter_names_available:
                selection = cls._query_filter_by_names(agg_sp, filt_sp_n)
            else:
                selection = cls._query_filter_by_codes(agg_sp, filt_sp_c)

        if condition_no_bbox:
            selection2 = cls._query_create_json_output()
        else:
            selection2 = cls._query_clip_bbox_create_json_output()

        query = selection + selection2

        if condition_filters_selected and condition_bbox_selected:
            if condition_filter_names_available:
                ret_val = cls._query_execute_filter_names_bbox(
                    query, filter_names, geometry_level, xmin, ymin, xmax, ymax, crs)
            else:
                ret_val = cls._query_execute_filter_codes_bbox(
                    query, filter_codes, geometry_level, xmin, ymin, xmax, ymax, crs)
        elif condition_filters_selected and condition_no_bbox:
            if condition_filter_names_available:
                ret_val = cls._query_execute_filter_names(
                    query, filter_names, geometry_level)
            else:
                ret_val = cls._query_execute_filter_codes(
                    query, filter_codes, geometry_level)
        elif condition_no_filters and condition_bbox_selected:
            ret_val = cls._query_execute_filter_bbox(
                query, agg_level, geometry_level, xmin, ymin, xmax, ymax, crs)
        else:
            ret_val = cls._query_execute_no_filter(
                query, agg_level, geometry_level)

        return ret_val.all()

    @validates_schema
    def validate_method(self, args, **kwargs):
        if "" == args.get('agg_level', ""):
            raise ValidationError(f"agg_level required for API call")
        if args["agg_level"] not in ['land', 'regierungsbezirk', 'kreis', 'verwaltungsgemeinschaft', 'gemeinde', 'nuts1', 'nuts2', 'nuts3']:
            raise ValidationError(
                f"Unknown agg_level {args['agg_level']}: must be land, regierungsbezirk, kreis, verwaltungsgemeinschaft, gemeinde, nuts1, nuts2 or nuts3")
        if "" == args.get('zoom_level', ""):
            raise ValidationError(f"zoom_level required for API call")
        if args["zoom_level"] < 0:
            raise ValidationError(
                f"Unknown zoom_level {args['zoom_level']}: must be between larger or equal to 0")
        if ((True not in args.get('filter_names', [True])) or (True not in args.get('filter_codes', [True]))) and (True == args.get('filter_level', True)):
            raise ValidationError(
                f"When a filter_name or a filter_code is selected a filter_level also must be defined")
        if ((True in args.get('filter_names', [True])) and (True in args.get('filter_codes', [True]))) and (False == args.get('filter_level', True)):
            raise ValidationError(
                f"Define either filter_names or filter_codes")
        if args.get('filter_level', 'land') not in ['land', 'regierungsbezirk', 'kreis', 'verwaltungsgemeinschaft', 'gemeinde', 'nuts1', 'nuts2', 'nuts3']:
            raise ValidationError(
                f"Unknown filter_level {args['filter_level']}: must be land, regierungsbezirk, kreis, verwaltungsgemeinschaft, gemeinde, nuts1, nuts2 or nuts3")

    @classmethod
    def fetch(cls, args):
        geometry_level = 10 - int(args.get('zoom_level', 2) - 1) // 1.1
        if geometry_level < 0:
            geometry_level = 0
        if geometry_level > 10:
            geometry_level = 10

        return cls._get_vg250(args['agg_level'],
                              geometry_level,
                              args.get('filter_level', ""),
                              args.get('filter_names', []),
                              args.get('filter_codes', []),
                              args.get('filter_boundingbox_southwest_lng', 0),
                              args.get('filter_boundingbox_southwest_lat', 0),
                              args.get('filter_boundingbox_northeast_lng', 0),
                              args.get('filter_boundingbox_northeast_lat', 0))[0][0]
