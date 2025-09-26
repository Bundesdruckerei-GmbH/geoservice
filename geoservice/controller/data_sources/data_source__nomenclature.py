# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from pathlib import Path
from typing import Optional, NamedTuple

import json
import pandas
from pandas import DataFrame
from sqlalchemy import delete, true

from geoservice.constants import PROJECT_ROOT
from geoservice.model import db
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.model.geoobject import LinkTable


class DataSourceNomenclature(DataSourceBase):
    CUSTOM_FLOW = True
    QUALITIES = {}
    MODEL = LinkTable
    _current_remote: str = ''
    _remote_path_lut: dict = {
        "DataSourceNomenclature": {
            "LUT": {
                "country-codes": "iso/country-codes.csv",
                "subdivision-names": "iso/subdivision-names.csv",
                "subdivision-categories": "iso/subdivision-categories.csv"
            }}
    }
    ADM_LEVEL = ''

    @classmethod
    def _local_storage_path(cls, qualities: NamedTuple) -> Path:
        # return RESOURCES_PATH / 'nomenclature' / f'{cls._current_remote}.csv'
        return PROJECT_ROOT / 'geoservice' / 'snippets' / 'nomenclature' / 'data' / f'{cls._current_remote}.csv'

    @classmethod
    def _remote_storage_path(cls, qualities: NamedTuple) -> str:
        return cls._cfg_remote_storage_path_lut()[cls._current_remote]

    @classmethod
    def _extract(cls, qualities: Optional[NamedTuple] = None, **kwargs) -> DataFrame:
        def _store(data_obj, path, cls, qualities):
            data_obj.to_csv(str(path), sep=",")

        return super()._extract(
            qualities=qualities,
            load_function=lambda path_or_buf, cls, qualities: pandas.read_csv(
                path_or_buf,
                sep=","
            ),
            store_function=_store
        )

    @classmethod
    def _transform(cls, data: DataFrame, qualities: Optional[NamedTuple] = None) -> DataFrame:
        return data

    @classmethod
    def _persist(cls, df: DataFrame, qualities: Optional[NamedTuple] = None) -> None:
        model = cls.MODEL or cls._model(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        db.session.execute(
            delete(model).where(
                model.link_to_aerial_level == cls.ADM_LEVEL,
                model.geometry_level == qualities._asdict().get('simplification_level', 0)
                if hasattr(model, 'geometry_level') else true()
            )
        )
        # - - - - - - - - - - - - - - - - - - - -
        db.session.add_all(list(map(lambda row: model(**row[1].to_dict()), df.iterrows())))
        db.session.commit()

    # todo discuss: adding custom extract flow

    @classmethod
    def _custom_etl_flow(cls, qualities: Optional[NamedTuple] = None):
        cls._current_remote = 'country-codes'
        country_codes = cls._extract(qualities, kwargs="country-codes.csv")

        cls._current_remote = 'subdivision-names'
        subdivision_names = cls._extract(qualities, kwargs="subdivision-names.csv")

        cls._current_remote = 'subdivision-categories'
        subdivision_categories = cls._extract(qualities, kwargs="subdivision-categories.csv")

        link_table = cls._transform([country_codes, subdivision_names, subdivision_categories])

        # statistics

        for adm_level in ["adm0", "adm1"]:
            cls.ADM_LEVEL = adm_level
            df = pandas.DataFrame(getattr(link_table, adm_level))
            cls._persist(df=df, qualities=qualities)
