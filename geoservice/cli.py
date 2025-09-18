from functools import reduce
from time import sleep

import click
from fsspec.registry import default

from .application import app
from .controller.data_sources.data_source__base import DataSourceBase


@app.cli.group(name='etl')
def etl_group():
    pass


def decorate_multiple(decorators):
    def decorator(func):
        return reduce(
            lambda agg, decorator: decorator(agg),
            decorators[::-1],
            func
        )
    return decorator


@etl_group.command()
@click.option('-s', '--sources', type=click.Choice([
    klass.__name__.replace('DataSource', '').lower()
    for klass in DataSourceBase.__subclasses__()
]), help='restrict which data sources should update', multiple=True)
@decorate_multiple([
    click.option(f'--{quality}')
    for quality in set([
        key
        for klass in DataSourceBase.__subclasses__()
        for key in klass.QUALITIES.keys()
    ])
])
def update(*args, **kwargs):
    quality_restrictions = {
        key: kwargs[key]
        for key in kwargs
        if key not in ['sources']
           and kwargs[key] is not None
    }
    # - - - - - - - - - - - - - - - - - - - -
    for data_source_klass in DataSourceBase.__subclasses__():
        data_source_klass.execute_update(
            quality_restrictions=quality_restrictions,
            datasource_restrictions=kwargs['sources']
        )


@etl_group.command()
@click.option('-s', '--sources', type=click.Choice([
    klass.__name__.replace('DataSource', '').lower()
    for klass in DataSourceBase.__subclasses__()
]), help='restrict which data sources should update', multiple=True)
def fetch(*args, **kwargs):
    for data_source_klass in DataSourceBase.__subclasses__():
        data_source_klass.execute_fetch_only(
            datasource_restrictions=kwargs['sources']
        )
        
