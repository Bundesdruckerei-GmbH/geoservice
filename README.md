# Geoservice

Geoservice aims to make "the official view of the german government on geographical topics" machine readable.

# Development

## Prerequisites

### Base

Make sure you have uv and npm installed. All other
dependencies will automatically be installed.

### Dependencies via Virtualenv

If you want to use plain virtualenv, no further preparation is required.

## Creating a logical database on postgres

make sure you have access to a postgres server (e.g. by starting one with
docker). Connect to the server, and create a logical database and user to access
it with these commands

    create user geoservice with password 'geoservice';
    create database geoservice with owner geoservice;

Afterwards the PostGIS Extension is required in the geoservice database for the processing of spatial data:

    create extension postgis;

Also the PostGIS Raster Extension is required in the geoservice database for the processing of raster data:

    create extension postgis_raster;

## Configure your connections

Copy `env.json.example` to `env.json`.

It should contain the environmental variables necessary to run the application given the definition we looked at earlier (however not necessarily containing the correct values!). Adjust the values to fit.

## Adding Testdata

### Preparing the database

To create the tables in the configured database, run

    uv run dev.py flask db upgrade

### Insert base data

To include new data in the geoservice, several steps has to be done. 

#### Define structure of the datasets in the database 

In the file [geoobject.py](geoservice/model/geoobject.py) already several classes are defined. If your dataset does not fit any of those classes, it is recommended to create a new class. The geoobject class already contains information about the geometry the source and so on. Similar to the other classes you can create a class based on the geoobject containing the additional required columns. 
For this definition a new version of the database needs to be created. Use alembic to create a new version file in [geoservice/model/migrations/versions](geoservice/model/migrations/versions) by calling

    uv run dev.py flask db migrate --rev-id REVISIONNUMBER -m YOUR_MESSAGE

Replace REVISIONNUMBER with a number higher than the highest number in [geoservice/model/migrations/versions](geoservice/model/migrations/versions) and replace YOUR_MESSAGE with a message of your choice. Afterwards check the automatically created file in the [geoservice/model/migrations/versions](geoservice/model/migrations/versions) folder for functionality and prune it to the required elements for the new dataset (look at the other migration files for help). Then upgrade the database as described above to include the new table structure. 

##### Example 

To represent an examplary test dataset stored in a gepackage with the name "testfile.gpkg" and the columns name, test_code, geometry_level, source and geometry in the database, the class definition could look like:

    class Testdata(Geoobject):
        test_code = db.Column(db.Unicode, nullable=False, default="")

The migration file, created by alembic, would - after some adaptions - contain the upgrade and downgrade functions:

    def upgrade():
        op.create_table('testdata',
        sa.Column('test_code', sa.Unicode(), nullable=False),
        sa.Column('name', sa.Unicode(), nullable=False),
        sa.Column('geometry_level', sa.Integer(), nullable=False),
        sa.Column('geometry', geoalchemy2.types.Geometry(srid=4326, dimension=2, from_text='ST_GeomFromEWKT', name='geometry'), nullable=True),
        sa.Column('source', sa.Unicode(), nullable=True),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('id')
        )

    def downgrade():
        op.drop_table('testdata')


#### Define ETL import of the data in the database

For each data source a file defines the ETL path. These are stored in  [geoservice/controller/data_sources](geoservice/controller/data_sources). The [data_source__base.py](geoservice/controller/data_sources/data_source__base.py) file contains a default way to import the data. For your data source you can create a file similar to the other source-specific files (data_source__YOURDATASOURCE.py). By calling 

    uv run dev.py flask etl update -s YOURDATASOURCE

the data are imported as defined in your file.

##### Example 

For the examplary dataset, the file could be named data_source__testdata.py and could contain the class DataSourceTestdata which reads the layer testfile from the testfile.gpkg and replaces the entries in the testdata table in the database where the source equals the adm_level and the geometry_level equals the simplification_level.   

    class DataSourceTestdata(DataSourceBase):

        QUALITIES: dict[list[Any]] = {
            'simplification_level': [0],
            'adm_level': ['testdata']
        }

        @classmethod
        def _local_storage_path(cls, qualities: NamedTuple) -> Path:
            return RESOURCES_PATH / 'testdata' / 'testfile.gpkg'

        @classmethod
        def _layer(cls, qualities: NamedTuple) -> str:
            return "testfile"

        @classmethod
        def _model(cls, qualities: NamedTuple) -> Type[Geoobject]:
            return Testdata

The data update can be started with the call

    uv run dev.py flask etl update -s testdata


#### Define API call of the data

The API call of the datasets is defined by the files in the [geoservice/schemas](geoservice/schemas) folder. Here you can create a schema for your new source similar to the aleardy existing source schemas. Afterwards you can import it in the [api.py](geoservice/controller/api.py) file and create a flask endpoint similar to the already existing ones.  

##### Example

The schema for the testdata could be stored in the file testdata_schema.py:

    class TestdataParameterSchema(Schema):
        name = fields.List(fields.Str())

        @classmethod
        def fetch(cls, args):
            test_query = select(Testdata).filter(Testdata.name.in_(args.get('name', "")))
            return geopandas.read_postgis(test_query, con=db.engine, geom_col='geometry')

After importing the schema to the api.py

    from ..schemas.testdata_schema import TestdataParameterSchema

the API endpoint could be created as:

    @blp.route("geo/test/", methods=["GET"])
    @blp.arguments(TestdataParameterSchema, location="query")
    def testget(args):
        response = make_response(
            TestdataParameterSchema().fetch(args).to_json(),
        )
        return response

## Start development server

Run

    uv run dev.py flask run

to start the development server on localhost:5000.

# Containerizing

The container should typically be created automatically by the build process.
If you want to create one by yourself, run first

    uv run dev.py build

to prepare the application.

Then, run

    docker build --tag=geoservice:dev .

to create the image and tag it with 'geoservice:dev'.

To test the image, make sure your postgres database is available from your ip,
then run

    docker run \
      --rm \
      $(cat env.json | jq -r 'to_entries[] | "-e \(.key)=\"\(.value)\""') \
      -p 3333:80 \
      geoservice:dev

to start it (the `$(cat ...)` part is a shortcut to transform the entries
env.json to -e KEY=VALUE options and is not strictly needed).

# Data Jobs

## Overview

Once the Geoservice is running, needs to be filled with data. This is achieved
via *Datajobs*. Datajobs are run in the kubernetes cluster. They have access to
the database, and are supposed to have all the relevant data contained in their
Docker images.

## Implementation and Testing

To introduce a new Datajob, start with a cli command, by adding an entrypoint
to `geoservice/cli.py`, like this:

    @app.cli.command()
    @click.argument("data", type=click.File("r"))
    def example_etl_job(data): 
        for item in json.load(data):
            db.session.add(
                Adm1(
                    adm0_code=data['adm0'],
                    adm1_code=data['adm1']))
        db.session.commit()


check locally if you can execute it successfully, e.g. with

    uv run dev.py flask example-etl-job example.json


# Legal Information
 
Copyright 2025 Bundesdruckerei GmbH
For the license see the accompanying LICENSE.md
