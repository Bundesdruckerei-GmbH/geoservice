# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

from sqlalchemy import delete
from geoalchemy2 import Geometry, Raster
from .base import Base, db
from ..application import app


class Geoobject(Base):
    __abstract__ = True
    name = db.Column(db.Unicode, nullable=False, default="")
    geometry_level = db.Column(db.Integer, nullable=False, default=0)
    geometry = db.Column(Geometry(srid=4326))
    source = db.Column(db.Unicode, nullable=True, default="")


class Adm0(Geoobject):
    adm0_code = db.Column(db.Unicode, nullable=False, default="")


class Adm1(Geoobject):
    adm0_code = db.Column(db.Unicode, nullable=False, default="")
    adm1_code = db.Column(db.Unicode, nullable=False, default="")
    adm0_name = db.Column(db.Unicode, nullable=False, default="")


class Consulates(Base):
    adm0_code = db.Column(db.Unicode, nullable=False, default="")
    sovereign_code = db.Column(db.Unicode, nullable=False, default="")
    consulate_code = db.Column(db.Unicode, nullable=False, default="")
    name_de = db.Column(db.Unicode, nullable=False, default="")
    name_en = db.Column(db.Unicode, nullable=False, default="")
    url = db.Column(db.Unicode, nullable=False, default="")
    source = db.Column(db.Unicode, nullable=True, default="")
    geometry = db.Column(Geometry(srid=4326))


class Wahlkreise(Base):
    adm1_code = db.Column(db.Unicode, nullable=False, default="")
    wkr_name = db.Column(db.Unicode, nullable=False, default="")
    wkr_nr = db.Column(db.Integer, nullable=False, default=0)
    land_name = db.Column(db.Unicode, nullable=False, default="")
    land_nr = db.Column(db.Integer, nullable=False, default=0)
    source = db.Column(db.Unicode, nullable=True, default="")
    geometry = db.Column(Geometry(srid=4326))


class Population(Base):
    adm0_code = db.Column(db.Unicode, nullable=False, default="")
    value = db.Column(db.Integer, nullable=False, default=0)
    year = db.Column(db.Integer, nullable=False, default=2024)
    source = db.Column(db.Unicode, nullable=True, default="")


class PopulationRaster(Base):
    rast = db.Column(Raster())


class VG250(Base):
    code = db.Column(db.Unicode, nullable=False, default="")
    name = db.Column(db.Unicode, nullable=False, default="")
    geometry_level = db.Column(db.Integer, nullable=False, default=0)
    agg_level = db.Column(db.Unicode, nullable=False, default="")
    source = db.Column(db.Unicode, nullable=True, default="")
    geometry = db.Column(Geometry(srid=4326))


class VG250Attributes(Base):
    arsg = db.Column(db.Unicode, nullable=False, default="")
    geng = db.Column(db.Unicode, nullable=False, default="")
    arsv = db.Column(db.Unicode, nullable=False, default="")
    genv = db.Column(db.Unicode, nullable=False, default="")
    arsk = db.Column(db.Unicode, nullable=False, default="")
    genk = db.Column(db.Unicode, nullable=False, default="")
    arsr = db.Column(db.Unicode, nullable=False, default="")
    genr = db.Column(db.Unicode, nullable=False, default="")
    arsl = db.Column(db.Unicode, nullable=False, default="")
    genl = db.Column(db.Unicode, nullable=False, default="")
    nuts1code = db.Column(db.Unicode, nullable=False, default="")
    nuts1name = db.Column(db.Unicode, nullable=False, default="")
    nuts2code = db.Column(db.Unicode, nullable=False, default="")
    nuts2name = db.Column(db.Unicode, nullable=False, default="")
    nuts3code = db.Column(db.Unicode, nullable=False, default="")
    nuts3name = db.Column(db.Unicode, nullable=False, default="")
    ewz = db.Column(db.Integer, nullable=False, default=0)
    source = db.Column(db.Unicode, nullable=True, default="")


class SettlingADM0(Base):
    adm0_code = db.Column(db.Unicode, nullable=False, default="")
    swx = db.Column(db.Integer, nullable=False, default=0)
    swy = db.Column(db.Integer, nullable=False, default=0)
    nex = db.Column(db.Integer, nullable=False, default=0)
    ney = db.Column(db.Integer, nullable=False, default=0)
    wsf_pop_factor = db.Column(db.Float, nullable=False, default=0)


class Metadata(Base):
    title = db.Column(db.Unicode, nullable=False, default="")
    abstract = db.Column(db.Unicode, nullable=False, default="")
    lineage = db.Column(db.Unicode, nullable=False, default="")
    responsibleParty = db.Column(db.Unicode, nullable=False, default="")
    crs = db.Column(db.Unicode, nullable=True, default="")
    format = db.Column(db.Unicode, nullable=False, default="")
    geoBox = db.Column(db.ARRAY(db.Numeric()), nullable=True, default="")
    datatype = db.Column(db.Unicode, nullable=False, default="")
    adaptionDate = db.Column(db.DateTime, nullable=True)
    source = db.Column(db.Unicode, nullable=True, default="")


class Metadatakeywords(Base):
    keywords = db.Column(db.Unicode(), nullable=True)
    source = db.Column(db.Unicode, nullable=True, default="")


class Metadataorigin(Base):
    originName = db.Column(db.Unicode(), nullable=False, default="")
    originSource = db.Column(db.Unicode(), nullable=False, default="")
    originAttribution = db.Column(db.Unicode(), nullable=False, default="")
    originLicence = db.Column(db.Unicode(), nullable=False, default="")
    originLicenceSource = db.Column(db.Unicode(), nullable=False, default="")
    originVersion = db.Column(db.Unicode(), nullable=False, default="")
    source = db.Column(db.Unicode, nullable=True, default="")


class PopulatedPlaces(Base):
    adm0_code = db.Column(db.Unicode, nullable=False, default="")
    capital_level = db.Column(db.Unicode, nullable=False, default="")
    nameascii = db.Column(db.Unicode, nullable=False, default="")
    name_de = db.Column(db.Unicode, nullable=False, default="")
    name_en = db.Column(db.Unicode, nullable=False, default="")
    name_fr = db.Column(db.Unicode, nullable=False, default="")
    population = db.Column(db.Integer, nullable=False, default=0)
    source = db.Column(db.Unicode, nullable=False, default="")
    geometry = db.Column(Geometry(srid=4326))


class LinkTable(Base):
    iso_name = db.Column(db.Unicode, nullable=False, default="")
    iso_3166_1_a2 = db.Column(db.Unicode, nullable=False, default="")
    iso_3166_1_a3 = db.Column(db.Unicode, nullable=False, default="")
    iso_3166_1_n3 = db.Column(db.Integer, nullable=False, default=0)
    independent = db.Column(db.Unicode, nullable=False, default="")
    iso_3166_2 = db.Column(db.Unicode, nullable=False, default="")

    link_to_aerial_level = db.Column(
        db.Unicode, nullable=False, default="")  # adm0|adm1
    # gadm|naturalearth|population|consulates
    link_to_source = db.Column(db.Unicode, nullable=False, default="")
    link_to_code = db.Column(db.Unicode, nullable=False,
                             default="")  # GER|DEU|XXO
    # Deutschland|...
    link_to_name = db.Column(db.Unicode, nullable=False, default="")
