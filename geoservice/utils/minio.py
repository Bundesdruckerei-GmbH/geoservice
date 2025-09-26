# Copyright 2025 Bundesdruckerei GmbH
# For the license, see the accompanying file LICENSE.md.

import logging

from attr import dataclass
from minio import Minio

import pyarrow.parquet as pq
import s3fs

from ..application import app


@dataclass
class MinioConfig(object):
    server: str
    access_key: str
    secret_key: str
    bucket: str


class MinioHelper:
    def __init__(self, objectname: str, config: MinioConfig = MinioConfig(
        server=app.config["MINIO_DEFAULT_SERVER"],
        access_key=app.config["MINIO_DEFAULT_ACCESS_KEY"],
        secret_key=app.config["MINIO_DEFAULT_SECRET_KEY"],
        bucket=app.config["MINIO_DEFAULT_BUCKET"]
    )):
        self._objectname = objectname
        self.response = None
        self.config = config
        self.logger = logging.getLogger('geoservice.minio_helper')

    def __enter__(self):
        client = Minio(
            self.config.server,
            self.config.access_key,
            self.config.secret_key,
            secure=app.config["MINIO_USE_TLS"],
        )
        self.response = client.get_object(self.config.bucket, self._objectname)
        self.logger.debug(f'Response status {self.response.status}')
        return self

    def __exit__(self, *args, **kwargs):
        self.response.close()
        self.response.release_conn()


@dataclass
class MinioParquetConfig(object):
    access_key: str
    secret_key: str
    bucket: str
    endpoint_url: str = app.config["MINIO_S3_ENDPOINT"]


class MinioParquetHelper:
    def __init__(self, objectname, config: MinioParquetConfig):
        self._objectname = objectname
        self.response = None
        self.config = config

    def __enter__(self):
        storage_options = {
            'anon': False,
            'client_kwargs': {
                'endpoint_url': self.config.endpoint_url,
                'aws_access_key_id': self.config.access_key,
                'aws_secret_access_key': self.config.secret_key
            }
        }
        filesystem = s3fs.S3FileSystem(**storage_options)
        dataset = pq.ParquetDataset(
            f's3a://{self.config.bucket}{self._objectname}',
            filesystem=filesystem
        ).read().to_pandas()

        return dataset

    def __exit__(self, *args, **kwargs):
        pass
