# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from flask_smorest import Blueprint
from flask import make_response
from io import BytesIO
from matplotlib.colors import ListedColormap

from ..application import flask_api, app

from ..schemas.geoobject_schema import GeoServiceImageArgs, GeoServiceArgs
from ..schemas.hillshade_schema import HillshadeParameterSchema
from ..schemas.landscan_schema import LandscanParameterSchema
from ..schemas.vg250_schema import VG250ParameterSchema
from ..schemas.population_schema import PopulationParameterSchema
from ..schemas.metadata_schema import MetadataParameterSchema


blp = Blueprint(
    name='api',
    import_name=__name__,
    url_prefix='/api'
)


@blp.route("/geo/", methods=["GET"])
@blp.arguments(GeoServiceArgs, location="query")
def api_geo(query_arguments):
    response = make_response(
        GeoServiceArgs.fetch(query_arguments).to_json(),
    )
    response.cache_control.max_age = 600;
    return response


@blp.route("/geo/svg/", methods=["GET"])
@blp.arguments(GeoServiceImageArgs, location="query")
def api_geo_svg(query_arguments):
    dataframe = GeoServiceArgs.fetch(query_arguments).set_index('adm0_code')

    dataframe["weight"] = 0.0
    for weight in query_arguments.get("filter_image_weights", []):
        dataframe.loc[weight["code"], "weight"] = weight["value"]

    app.logger.info(dataframe)
    image = dataframe.to_crs("EPSG:4087").plot(column="weight", cmap=ListedColormap(["#E0E0E0", "#CCE4F0", "#99C9E2", "#66ADD3", "#3392C5", "#0077B6"]))
    image.axis('off')
    image.margins(0, 0)

    buffer = BytesIO()
    image.get_figure().savefig(buffer, format='svg', bbox_inches='tight', pad_inches=0)

    buffer.seek(0)
    response = make_response(buffer.getvalue())
    response.mimetype = 'image/svg+xml'

    return response


@blp.route("geo/vg250/", methods=["GET"])
@blp.arguments(VG250ParameterSchema, location="query")
def api_geo_vg250(args):
    return VG250ParameterSchema().fetch(args)


@blp.route("geo/population/", methods=["GET"])
@blp.arguments(PopulationParameterSchema, location="query")
def api_geo_population(args):
    return PopulationParameterSchema().fetch(args)


@blp.route("geo/metadata/", methods=["GET"])
@blp.arguments(MetadataParameterSchema, location="query")
def api_geo_metadata(args):
    response = make_response(MetadataParameterSchema().fetch(args))
    response.headers['Content-Type'] = 'application/json'
    return response


@blp.route("geo/landscan", methods=["GET"])
@blp.arguments(LandscanParameterSchema, location="query")
def api_landscan(args):
    return LandscanParameterSchema.fetch(args)


@blp.route("geo/hillshade", methods=["GET"])
@blp.arguments(HillshadeParameterSchema, location="query")
def api_hillshade(args):
    return HillshadeParameterSchema.fetch(args)


flask_api.register_blueprint(blp)
