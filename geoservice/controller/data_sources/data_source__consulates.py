# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

import json
from typing import NamedTuple, Optional

import geopandas
from geopandas import GeoDataFrame
import pandas
from shapely import Point
from thefuzz import fuzz
from sqlalchemy import delete
from urllib3.response import HTTPResponse

from geoservice.constants import RESOURCES_PATH
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.model import db
from geoservice.model.geoobject import Consulates


class DataSourceConsulates(DataSourceBase):
    CUSTOM_FLOW = True
    QUALITIES = {}
    MODEL = Consulates
    EPITHETS = [
        "German",
        "Embassy",
        "Consulate",
        "General",
        "Liaison",
        "Office",
        "Institute",
        "Federal",
        "Foreign",
        "Amt",
        "Deutsche",
        "Deutsches",
        "Botschaft",
        "Generalkonsulat",
        "Auswärtiges",
        "Konsulat",
        "Vertretungsbüro"
    ]
    DISTINCTIVE_ALLOCATIONS = [
        {
            "consulate_attribute": "name_en",
            "consulate_naming": "The Holy See",
            "df_attribute": "NAME",
            "df_naming": "Vatican City"
        },
        {
            "consulate_attribute": "name_en",
            "consulate_naming": "Osaka-Kobe",
            "df_attribute": "NAMEASCII",
            "df_naming": "Osaka"
        },
        {
            "consulate_attribute": "name_en",
            "consulate_naming": "San Francisco",
            "df_attribute": "NAMEALT",
            "df_naming": "San Francisco-Oakland"
        },
        {
            "consulate_attribute": "name_en",
            "consulate_naming": "St. Petersburg",
            "df_attribute": "NAMEALT",
            "df_naming": "Sankt Peterburg|Saint Petersburg"
        },
        {
            "consulate_attribute": "name_en",
            "consulate_naming": "Athens",
            "df_attribute": "NAMEALT",
            "df_naming": "Athinai"
        },
    ]
    _current_remote: str = ''

    @classmethod
    def _layer(cls, qualities: NamedTuple) -> str:
        return "ne_10m_populated_places"

    @classmethod
    def _remote_storage_path(cls, qualities: NamedTuple) -> str:
        return cls._cfg_remote_storage_path_lut()[cls._current_remote]

    @classmethod
    def _transform_consulates(cls, consulates):
        def extract_location_name(string):
            word_list = string.split(" ")
            epithets = " ".join([word for word in word_list if word in cls.EPITHETS])
            return string.replace(epithets, "").strip()
        # - - - - - - - - - - - - - - - - - - - -
        consulates_processed = []
        for consulate in consulates:
            consulate_processed = {}
            # - - - - - - - - - - - - - - - - - - - -
            if consulate["code"] == "AA":
                continue
            # - - - - - - - - - - - - - - - - - - - -
            if any([value is None for value in consulate.values()]):
                # todo: catch this case
                continue
            # - - - - - - - - - - - - - - - - - - - -
            consulate_processed["consulate_code"] = consulate["code"]
            location_name_en = extract_location_name(consulate["name_en"])
            # - - - - - - - - - - - - - - - - - - - -
            if not location_name_en:
                cls.logger.info(f"No place name found for consulate {consulate['name_en']}. Dropping it.")
            else:
                consulate_processed["name_en"] = location_name_en
            # - - - - - - - - - - - - - - - - - - - -
            location_name_de = extract_location_name(consulate["name_de"])
            if location_name_de:
                consulate_processed["name_de"] = location_name_de
            # - - - - - - - - - - - - - - - - - - - -
            # todo: potentially also city_status
            consulate_processed["url"] = consulate["URL"]
            consulates_processed.append(consulate_processed)
        # - - - - - - - - - - - - - - - - - - - -
        return consulates_processed

    @classmethod
    def _in_depth_match(cls, list_element, dataframe, highscore):
        matching_row = None
        for _, row in dataframe.iterrows():
            for name, value in row.to_dict().items():
                if "NAME_" in name or "NAMEASCII" in name:
                    actual_score = fuzz.ratio(str(value), list_element["name_en"])
                    if highscore < actual_score:
                        highscore = actual_score
                        matching_row = row
                    if highscore == 100:
                        return highscore, matching_row
        return highscore, matching_row

    @classmethod
    def _add_consulate_infos(cls, consolidated_data, list_element, df_element):
        if list_element and isinstance(df_element, pandas.Series):
            return consolidated_data.append(
                {
                    "adm0_code": df_element["ADM0_A3"],
                    "sovereign_code": df_element["SOV_A3"],
                    "consulate_code": list_element["consulate_code"],
                    "name_en": list_element["name_en"],
                    "name_de": list_element["name_de"],
                    "url": list_element["url"],
                    "source": "consulates",
                    "geometry": Point(df_element["geometry"]),
                }
            )

    @classmethod
    def _merge_data(cls, consulate_list, populated_places):
        consolidated_data = []
        # - - - - - - - - - - - - - - - - - - - -
        for idx, element in enumerate(consulate_list):
            highscore = 0
            matching_row = None
            # - - - - - - - - - - - - - - - - - - - -
            consulate = next((
                consulate
                for consulate in cls.DISTINCTIVE_ALLOCATIONS
                if consulate["consulate_naming"] == element[consulate["consulate_attribute"]]
            ), None)
            # - - - - - - - - - - - - - - - - - - - -
            if consulate:
                result = populated_places[populated_places[consulate["df_attribute"]] == consulate["df_naming"]]
                matching_row = result.iloc[0]
                highscore = 100
            else:
                for _, row in populated_places.iterrows():
                    if highscore == 100:
                        continue
                    actual_score = fuzz.ratio(row["NAME_EN"], element["name_en"])
                    if actual_score > highscore:
                        matching_row = row
                        highscore = actual_score

                if highscore <= 90:
                    cls.logger.debug(
                        f"Shallow match results in a score of {highscore} ({element['name_en']}, row {idx}).\n"
                        f"Performing in-depth match")
                    highscore, matching_row = cls._in_depth_match(element, populated_places, highscore)
            # - - - - - - - - - - - - - - - - - - - -
            cls._add_consulate_infos(consolidated_data, element, matching_row)
            # - - - - - - - - - - - - - - - - - - - -
            cls.logger.info(f"Coordinates added to consulates data ({element['name_en']}, row {idx})")
            cls.logger.debug(f"Final score: {highscore} \n")
        # - - - - - - - - - - - - - - - - - - - -
        return geopandas.GeoDataFrame(consolidated_data)

    @classmethod
    def _custom_extract_flow(cls, qualities: Optional[NamedTuple] = None):
        cls._current_remote = 'consulates'
        def _store(data_obj, path, cls, qualities):
            with open(path, 'w') as outfile:
                json.dump(data_obj, outfile)

        consulates = cls._extract(
            qualities=qualities,
            local_storage_path=RESOURCES_PATH / 'consulates' / 'consulates.json',
            load_function=lambda source, cls, qualities: (
                source.json()
                if isinstance(source, HTTPResponse) else
                json.load(open(source))
            ),
            store_function=_store
        )
        # - - - - - - - - - - - - - - - - - - - -
        cls._current_remote = 'populated_places'
        populated_places = cls._extract(
            qualities=qualities,
            local_storage_path=RESOURCES_PATH / 'naturalearth' / 'populated_places.gpkg',
            load_function=lambda path, cls, qualities: geopandas.read_file(
                path,
                driver=cls.ENGINE,
            ),
        )
        # - - - - - - - - - - - - - - - - - - - -
        return consulates, populated_places


    @classmethod
    def _persist(cls, gdf: GeoDataFrame, qualities: Optional[NamedTuple] = None) -> None:
        model = cls.MODEL or cls._model(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        db.session.execute(
            delete(model).where(
                model.source == 'consulates',
            )
        )
        # - - - - - - - - - - - - - - - - - - - -
        db.session.add_all(list(map(lambda row: model(**row[1].to_dict()), gdf.iterrows())))
        db.session.commit()

    @classmethod
    def _custom_etl_flow(cls, qualities: Optional[NamedTuple] = None):
        consulates, populated_places = cls._custom_extract_flow(qualities)
        # - - - - - - - - - - - - - - - - - - - -
        consulates__transformed = cls._transform_consulates(consulates)
        merged_data = cls._merge_data(consulates__transformed, populated_places)
        # - - - - - - - - - - - - - - - - - - - -
        cls._sql_update_bbox('consulates', merged_data, qualities)
        cls._sql_update_crs('consulates', populated_places, qualities)
        cls._sql_update_metadatastate('consulates', qualities)
        # - - - - - - - - - - - - - - - - - - - -
        cls._persist(gdf=merged_data, qualities=qualities)
