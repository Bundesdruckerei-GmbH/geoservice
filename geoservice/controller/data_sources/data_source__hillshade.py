# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

import os
import subprocess
from urllib3 import HTTPResponse
from typing import Optional, NamedTuple

from geoservice.constants import RESOURCES_PATH
from geoservice.controller.data_sources.data_source__base import DataSourceBase
from geoservice.utils.shell_utils import find_exe


class DataSourceHillshade(DataSourceBase):

    CUSTOM_FLOW = True
    LOCAL_STORAGE_PATH = RESOURCES_PATH / 'hillshade' / 'SR_LR.tif'
    QUALITIES = {}

    @classmethod
    def _custom_extract_flow(cls, qualities: Optional[NamedTuple] = None):
        def _store(data_obj, path, cls, qualities):
            with open(path, 'wb') as outfile:
                outfile.write(data_obj)
        # - - - - - - - - - - - - - - - - - - - -
        return cls._extract(
            qualities=qualities,
            load_function=lambda response, cls, qualities: (
                response.read() if isinstance(response, HTTPResponse) else open(response, 'rb').read()
            ),
            store_function=_store,
        )

    @classmethod
    def _custom_etl_flow(cls, qualities: Optional[NamedTuple] = None):
        from geoservice import app
        # - - - - - - - - - - - - - - - - - - - -
        cls._custom_extract_flow(qualities=qualities)
        # - - - - - - - - - - - - - - - - - - - -
        subprocess.run(
            " ".join([
                find_exe('raster2pgsql', os.environ),
                '-d',  # drop tables if exists
                '-e',  # no transaction
                '-I',  # create spatial index
                '-C',  # set raster constraints
                '-M',  # vacuum and analyze after load
                '-Y', '50',  # batch processing
                '-t', 'auto',  # block size same as tif
                str(cls.LOCAL_STORAGE_PATH),
                'hillshade',  # target table name
                '|',  # - - - - - - - - - - - - - - - - - - - -
                'psql',
                '-h', f'{app.config["DATABASE_HOST"]}',
                '-U', f'{app.config["DATABASE_USER"]}',
                '-p', f'{app.config["DATABASE_PORT"]}',
                '-d', f'{app.config["DATABASE_NAME"]}'
            ]),
            check=True,
            shell=True,
            env=dict(os.environ, **{"PGPASSWORD": app.config["DATABASE_PASSWORD"]}),
            capture_output=True,
        )
