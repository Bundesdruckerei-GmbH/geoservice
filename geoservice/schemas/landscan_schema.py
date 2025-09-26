# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from io import BytesIO
from sqlalchemy import text

from marshmallow import Schema, fields
from flask import send_file

from geoservice.model import db


class LandscanParameterSchema(Schema):
    filter_boundingbox_southwest_lat = fields.Float()
    filter_boundingbox_southwest_lng = fields.Float()
    filter_boundingbox_northeast_lat = fields.Float()
    filter_boundingbox_northeast_lng = fields.Float()

    @classmethod
    def fetch(cls, args):
        return send_file(
            BytesIO(db.session.execute(text(f'''
                WITH raster_selection AS (
                    SELECT ST_Clip(
                        rast, 
                        ST_MakeEnvelope(
                            {args["filter_boundingbox_southwest_lng"]}, 
                            {args["filter_boundingbox_southwest_lat"]}, 
                            {args["filter_boundingbox_northeast_lng"]}, 
                            {args["filter_boundingbox_northeast_lat"]}, 
                            4326
                        )
                    ) as rast
                    FROM landscan as rasterdata
                    WHERE ST_Intersects(rasterdata.rast, ST_MakeEnvelope(
                        {args["filter_boundingbox_southwest_lng"]}, 
                        {args["filter_boundingbox_southwest_lat"]}, 
                        {args["filter_boundingbox_northeast_lng"]}, 
                        {args["filter_boundingbox_northeast_lat"]}, 
                        4326
                    ))
                ),
                unified_raster AS (
                    SELECT ST_Union(rast,'MAX') AS rast
                    FROM raster_selection
                )
                SELECT ST_AsTIFF(rast) from unified_raster
            ''')).scalar_one()),
            mimetype='image/tif',
            as_attachment=False,
            download_name=(
                f"landscan_"
                f"{args['filter_boundingbox_southwest_lat']}_"
                f"{args['filter_boundingbox_southwest_lng']}_"
                f"{args['filter_boundingbox_northeast_lat']}_"
                f"{args['filter_boundingbox_northeast_lng']}.tiff"
            ))
