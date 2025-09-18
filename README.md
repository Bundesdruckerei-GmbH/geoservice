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

TO BE DONE

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

