# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.


from marshmallow import Schema, fields
from sqlalchemy import select, distinct
import pandas

from ..model import db
from ..model.geoobject import Metadata, Metadatakeywords, Metadataorigin


class MetadataParameterSchema(Schema):
    source = fields.List(fields.Str())
    available_sources = fields.Boolean()
    filter_boundingbox_southwest_lat = fields.Float()
    filter_boundingbox_southwest_lng = fields.Float()
    filter_boundingbox_northeast_lat = fields.Float()
    filter_boundingbox_northeast_lng = fields.Float()

    @classmethod
    def _load_additional_metadata(cls, sources: list) -> pandas.DataFrame:
        """
        This function loads keywords and origins of metadata
        """
        select_keywords = select(
            Metadatakeywords.keywords, Metadatakeywords.source)

        select_origin = select(
            Metadataorigin.originName,
            Metadataorigin.originSource,
            Metadataorigin.originAttribution,
            Metadataorigin.originLicence,
            Metadataorigin.originLicenceSource,
            Metadataorigin.originVersion,
            Metadataorigin.source)

        keywords_metadata = pandas.read_sql(
            select_keywords.filter(Metadatakeywords.source.in_(sources)
                                   ),
            con=db.engine
        )
        keywords_metadata = keywords_metadata.groupby(
            'source')['keywords'].apply(list).reset_index()

        origin_metadata = pandas.read_sql(
            select_origin.filter(Metadataorigin.source.in_(sources)
                                 ),
            con=db.engine
        )
        origin_metadata['origin'] = origin_metadata.apply(
            lambda row: dict(row[[
                'originName',
                'originSource',
                'originAttribution',
                'originLicence',
                'originLicenceSource',
                'originVersion']]), axis=1)
        origin_metadata = origin_metadata[['origin', 'source']].groupby(
            'source')['origin'].apply(list).reset_index()
        additional_metadata = keywords_metadata.merge(
            origin_metadata, how='left', on='source')

        return additional_metadata

    @classmethod
    def fetch(cls, args):
        # Return only the available sources
        if args.get('available_sources', False):
            return pandas.read_sql(
                select(
                    distinct(Metadata.source)
                ),
                con=db.engine
            ).to_json(orient="columns")

        select_all_variables = select(
            Metadata.title,
            Metadata.abstract,
            Metadata.lineage,
            Metadata.responsibleParty,
            Metadata.crs,
            Metadata.format,
            Metadata.geoBox,
            Metadata.datatype,
            Metadata.adaptionDate,
            Metadata.source
        )

        # Return all data
        if args.get('source', [""]) == [""]:
            # Return all data intersecting with bbox
            if all([type(args.get('filter_boundingbox_southwest_lat', False)) is not bool,
                    type(args.get('filter_boundingbox_northeast_lat', False)) is not bool,
                    type(args.get('filter_boundingbox_southwest_lng', False)) is not bool,
                    type(args.get('filter_boundingbox_northeast_lng', False)) is not bool]):
                select_all_variables = select_all_variables.filter(
                    Metadata.geoBox[4] > args.get(
                        'filter_boundingbox_southwest_lat', 0)
                ).filter(
                    Metadata.geoBox[2] < args.get(
                        'filter_boundingbox_northeast_lat', 0)
                ).filter(
                    Metadata.geoBox[3] > args.get(
                        'filter_boundingbox_southwest_lng', 0)
                ).filter(
                    Metadata.geoBox[1] < args.get(
                        'filter_boundingbox_northeast_lng', 0)
                )
            base_metadata = pandas.read_sql(
                select_all_variables,
                con=db.engine
            )
            return base_metadata.merge(
                cls._load_additional_metadata(
                    base_metadata.source.to_list()),
                how='left',
                on='source').to_json(orient="records", date_format="iso")

        # Return selected data intersecting with bbox
        if all([type(args.get('filter_boundingbox_southwest_lat', False)) is not bool,
                type(args.get('filter_boundingbox_northeast_lat', False)) is not bool,
                type(args.get('filter_boundingbox_southwest_lng', False)) is not bool,
                type(args.get('filter_boundingbox_northeast_lng', False)) is not bool]):
            base_metadata = pandas.read_sql(
                select_all_variables.filter(
                    Metadata.source.in_(args.get('source', [""]))
                ).filter(
                    Metadata.geoBox[4] > args.get(
                        'filter_boundingbox_southwest_lat', 0)
                ).filter(
                    Metadata.geoBox[2] < args.get(
                        'filter_boundingbox_northeast_lat', 0)
                ).filter(
                    Metadata.geoBox[3] > args.get(
                        'filter_boundingbox_southwest_lng', 0)
                ).filter(
                    Metadata.geoBox[1] < args.get(
                        'filter_boundingbox_northeast_lng', 0)
                ),
                con=db.engine
            )
            return base_metadata.merge(
                cls._load_additional_metadata(
                    base_metadata.source.to_list()),
                how='left',
                on='source').to_json(orient="records", date_format="iso")

        # Return selected data
        base_metadata = pandas.read_sql(
            select_all_variables.filter(Metadata.source.in_(args.get('source', [""]))
                                        ),
            con=db.engine
        )

        return base_metadata.merge(
            cls._load_additional_metadata(
                base_metadata.source.to_list()),
            how='left',
            on='source').to_json(orient="records", date_format="iso")
