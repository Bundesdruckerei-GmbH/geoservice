import json
import logging
import os
import ssl
from typing import Optional

from flask import Flask
from flask_cors import CORS
from flask_smorest import Api
from jinja2 import StrictUndefined
from werkzeug.middleware.proxy_fix import ProxyFix

from .buildinfo import version
from .logging import setup_logging


class Application(Flask):
    """
    The main application and WSGI-Entrypoint.

    Holds the configuration and middleware.

    The configuration is extracted from environment variables starting with
    GEOSERVICE_, and passed as keywords into `update_config`. For background, see
    https://12factor.net/config.

    """

    # undefined variables in the template should not be silently ignored
    jinja_options = {"undefined": StrictUndefined}

    def __init__(self):
        Flask.__init__(self, __name__)

        # this enables the application to run behind a forward proxy, by
        # making it respect `X-Forwarded-Host` and similar headers.
        # see https://werkzeug.palletsprojects.com/en/2.0.x/middleware/proxy_fix/
        self.wsgi_app = ProxyFix(self.wsgi_app)

        # don't swallow exceptions quietly, log them into stderr
        self.config.update({"PROPAGATE_EXCEPTIONS": True})

        # some variables should be available in every possible template,
        # regardless of controller. These are defined here.
        @self.context_processor
        def add_meta():
            return {
                "version": version,
            }

    def update_config(
        self,
        database_type: str = "",
        database_host: str = "",
        database_port: str = "",
        database_user: str = "",
        database_password: str = "",
        database_database: str = "",
        database_tls: str = "",
        database_path: str = "",
        default_minio_server: str = "",
        minio_s3_endpoint: str = "",
        default_minio_access_key: str = "",
        default_minio_secret_key: str = "",
        minio_default_bucket: str = "test",
        minio_use_tls: str = "True",
        debug: str = "False",
        secret_key: Optional[str] = None,
        runconfig_loglevel: int = logging.INFO,
        local_runtime: str = "false",
        etl_allow_failure: str = "false",
        etl_pull_missing_files_for_local_runtime: str = "false",
        etl_remote_sources="{}",
        etl_remote_sources_secrets="{}",
        **kwargs
    ):

        """updates the config with the given parameters.

        the parameters are usually extracted from the environment (see below, and
        `get_environment_config`).

        new parameters *must* have default values, otherwise existing deployments
        may fail.

        debug:
            whether the debug mode should be activated. Every value not case
            insensitively equal to the string 'true' is considered false.

        database_type:
            The database to use. Currently supported are 'hana', 'sqlilte'

        database_host:
            The host of the database, e.g. 'hana.example.com'

        database_port:
            The port of the database, e.g. '30015'

        database_user:
            The user to connect to the database, e.g. 'alice'

        database_password:
            The password to connect to the database, e.g. 'secret'

        database_database:
            The logical database on the server to connect to, e.g. 'acled'

        database_tls:
            whether to use TLS to connect to the database, e.g. 'true'

        database_path:
            The path to the database, e.g. '/path/to/database.db' (only applies to sqlite)

        secret_key:
            secret, random value, used in generation of client side sessions

        loglevel:
            log level to apply to the application logger

        local_runtime:
            determines if current runtime environment is local

        """
        debug = debug.lower() == "true"
        local_runtime = local_runtime.lower() == "true"

        self.config["SECRET_KEY"] = secret_key
        self.config["DEBUG"] = debug
        self.config["LOCAL_RUNTIME"] = local_runtime

        # Database
        if database_type == "postgres":
            ssl_context = (ssl.SSLContext()
                                if database_tls.lower() == "true"
                                else None)
            self.config.update({
                "SQLALCHEMY_DATABASE_URI":
                    f"postgresql+pg8000://{database_user}:{database_password}@{database_host}:{database_port}/{database_database}",
                "SQLALCHEMY_ENGINE_OPTIONS": {
                    'connect_args': {'ssl_context': ssl_context},
                    'pool_recycle': 300,
                },
                "DATABASE_USER": database_user,
                "DATABASE_PASSWORD": database_password,
                "DATABASE_HOST": database_host,
                "DATABASE_PORT": database_port,
                "DATABASE_NAME": database_database
            })
        elif database_type == "sqlite":
            self.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{database_path}"
        else:
            self.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///:memory:"
            self.logger.critical(f"Unknown database type '{database_type}', fallback to in-memory db")

        self.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = debug

        # Minio
        self.config["MINIO_DEFAULT_SERVER"] = default_minio_server
        self.config["MINIO_S3_ENDPOINT"] = minio_s3_endpoint
        self.config["MINIO_DEFAULT_ACCESS_KEY"] = default_minio_access_key
        self.config["MINIO_DEFAULT_SECRET_KEY"] = default_minio_secret_key
        self.config["MINIO_DEFAULT_BUCKET"] = minio_default_bucket
        self.config["MINIO_USE_TLS"] = minio_use_tls.lower() == "true"

        # ETL
        self.config["ETL_ALLOW_FAILURE"] = etl_allow_failure.lower() == "true"
        self.config["ETL_PULL_MISSING_FILES_FOR_LOCAL_RUNTIME"] = etl_pull_missing_files_for_local_runtime.lower() == "true"
        self.config["ETL_REMOTE_SOURCES"] = json.loads(etl_remote_sources)
        self.config["ETL_REMOTE_SOURCES_SECRETS"] = json.loads(etl_remote_sources_secrets) \
            if etl_remote_sources_secrets else {}

        # Logging
        setup_logging(runconfig_loglevel, debug=debug)

        # OpenAPI
        app.config["API_TITLE"] = "geoservice"
        app.config["API_VERSION"] = version
        app.config["OPENAPI_VERSION"] = "3.0.0"
        app.config["OPENAPI_URL_PREFIX"] = '/openapi'

        self.config["JWT_AUTH_URL_RULE"] = None

        for key in kwargs:
            self.logger.warning(f"'{key}' configured but not used")


def get_environment_config():
    """extracts all environment variables with a 'GEOSERVICE_' prefix

    returns them lowercased and without prefix as a dictionary
    """
    prefix = "GEOSERVICE_"
    return dict(
        (key.lower()[len(prefix):], value)
        for key, value in os.environ.items()
        if key.startswith(prefix)
    )


app = Application()
app.update_config(**get_environment_config())

flask_api = Api(app)

cors = CORS(app)
