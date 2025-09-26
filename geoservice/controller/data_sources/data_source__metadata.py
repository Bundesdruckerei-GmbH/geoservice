# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from typing import Optional, NamedTuple, Any, Optional, NamedTuple, Callable
from pathlib import Path

import pandas
from sqlalchemy import delete

from geoservice.constants import RESOURCES_PATH
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.model.geoobject import Metadata, Metadatakeywords, Metadataorigin
from geoservice.model import db


class DataSourceMetadata(DataSourceBase):

    QUALITIES = {
        'source': ['consulates',
                   'gadm',
                   'naturalearth',
                   'population',
                   'wahlkreise',
                   'vg250']
    }
    ENGINE = 'pyarrow'
    MODEL = Metadata

    @classmethod
    def _local_storage_path(cls, qualities: NamedTuple) -> Path:
        metafile = qualities.source + '.json'
        return RESOURCES_PATH / 'metadata' / metafile

    @classmethod
    def _remote_storage_path(cls, qualities: NamedTuple):
        from geoservice import app
        metafile = f'{qualities.source}{".json"}'
        metapath = (app.config['ETL_REMOTE_SOURCES'].get(cls.__name__, {'PATH': None}).get('PATH', None)
                    or app.config.get(f'MINIO_DEFAULT_PATH', None))
        metapath = f'{metapath}{metafile}'
        return (metapath)

    @classmethod
    def _cfg_remote_storage_path(cls) -> bool:
        return False

    @classmethod
    def _extract(
        cls,
        qualities: Optional[NamedTuple] = None,
        local_storage_path: Path = None,
        load_function: Callable[[Path, type, Optional[NamedTuple]], Any] = (
            lambda path, cls, qualities: pandas.DataFrame.from_dict(
                pandas.read_json(path_or_buf=path,
                                 engine=cls.ENGINE,
                                 lines=True)["0"][0],
                orient='index').transpose()
        ),
        store_function: Callable[[Any, Any, type, Optional[NamedTuple]], None] = (
            lambda data_obj, source, cls, qualities:
            data_obj.to_json(path_or_buf=str(source), orient="index")
        ),
        fetch_mode: bool = False
    ) -> Any:
        return super()._extract(qualities,
                                local_storage_path,
                                load_function,
                                store_function,
                                fetch_mode)

    @classmethod
    def _persist(cls, gdf: pandas.DataFrame, qualities: Optional[NamedTuple] = None) -> None:
        mainmodel = Metadata
        keymodel = Metadatakeywords
        originmodel = Metadataorigin

        mainmeta = gdf[[x for x in gdf.columns.to_list() if x in [
            "title",
            "abstract",
            "lineage",
            "responsibleParty",
            "crs",
            "format",
            "geoBox",
            "datatype",
            "source"]]]
        keymeta = pandas.DataFrame(
            {'keywords': pandas.DataFrame.from_dict(gdf["keywords"][0])[0].to_list(),
             'source': gdf["source"][0]})
        originmeta = pandas.DataFrame.from_records(gdf["origin"][0])
        originmeta["source"] = gdf["source"][0]

        for model, metadata in zip([mainmodel, keymodel, originmodel],
                                   [mainmeta, keymeta, originmeta]):
            # - - - - - - - - - - - - - - - - - - - -
            db.session.execute(
                delete(model).where(
                    model.source == qualities.source
                )
            )
            # - - - - - - - - - - - - - - - - - - - -
            db.session.add_all(
                list(map(lambda row: model(**row[1].to_dict()),
                         metadata.iterrows())))
            db.session.commit()
