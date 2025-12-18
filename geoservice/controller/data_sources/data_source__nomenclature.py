# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from pathlib import Path
from typing import Optional, NamedTuple

import json
import pandas
from pandas import DataFrame
from sqlalchemy import delete, true

import geoservice
from geoservice.constants import RESOURCES_PATH, PROJECT_ROOT
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
                "iso_fixture_adm0": "iso_fixture_adm0.json",
                "iso_fixture_adm1": "iso_fixture_adm1.json"
            }}
    }
    ADM_LEVEL = ''


    @classmethod
    def _local_storage_path(cls, qualities: NamedTuple) -> Path:
        return RESOURCES_PATH / 'nomenclature' / f'{cls._current_remote}.json'

    @classmethod
    def _remote_storage_path(cls, qualities: NamedTuple) -> str:
        return cls._cfg_remote_storage_path_lut()[cls._current_remote]


    @classmethod
    def _extract(cls, qualities: Optional[NamedTuple] = None, **kwargs) -> DataFrame:
        def _store(data_obj, path, cls, qualities):
            with open(path, 'w') as file:
                json.dump(data_obj, file, ensure_ascii=False)
        
        def _load(path, cls, qualities):
            with open(path, "r") as read_file:
                return json.load(read_file)

        return super()._extract(
            qualities=qualities,
            load_function=_load,
            store_function=_store
        )  

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


    @classmethod
    def _custom_etl_flow(cls, qualities: Optional[NamedTuple] = None):
        for adm_level in ["adm0", "adm1"]:
            cls._current_remote = f'iso_fixture_{adm_level}'
            cls.ADM_LEVEL = adm_level
            dataset = cls._extract(qualities, kwargs=f'{cls._current_remote}.json')
            df = pandas.DataFrame(dataset)
            cls._persist(df=df, qualities=qualities)
