# Geoservice

## Introduction

The geoservice provides a standalone backend service that developers and analysts can use to obtain geoinformation for their applications. A wide variety of output formats are supported to deliver the three data categories of geometries, rasters and attributes.

### Goals

- Persistence of various GEO data sets with an ETL route
- Provision of a REST service for consuming the GEO data sets
- Provision of a front-end component for automated connection to the REST service

### Use and support

- User documentation is part of the runtime environment and can be accessed via the front page
- Support & communication channels: Please use feature requests via Github

## Relevant design and architectural decisions

- Backend technology: Python
- Persistence technology: PostGIS
- Frontend technology: JavaScript / Leaflet

## Components

### OSS-Backend

Contains the Python backend for the service, access to the database, and the framework for the ETL pipeline.

### OSS-Frontend

Contains the JS library for connecting a map front end such as OpenLayer or Leaflet to the GeoService back end.

### Database

PostGIS database. The data is generally processed using libraries such as GeoPandas or MapShaper and imported into the database.
The database contains data optimised for queries. Live geofunctions during retrieval are covered by PostGIS-specific commands.

### Other interfaces

All ETL processes start from data stored in local directories. Extraction is therefore limited to local resources.
The transformations can be found for each data source under geoservice/controller/data_sources.

## Transverse concepts

### Licences

- Restrictions by OSS components in the backend or frontend are harmless
- PostGIS DB: GPL, does not infect the application
- Map data sets: To be considered on a case-by-case basis, does not infect the application

### Monitoring

The geoservice provides an integration test as a health check under the relative URL /monitoring/.

If successful, status code 200 is returned. If an error occurs, status code 500 is returned and the return body lists
the failed tests.

This endpoint can be configured in the standard monitoring tool, allowing the functionality of the application to be
proactively monitored.

## Development and Maintenance

### Setting up the development environment

- Repository Clone
- Follow the instructions in README.md
