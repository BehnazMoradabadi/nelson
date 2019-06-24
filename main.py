import logging
import os

import click
from flask import Flask


from parsers.base import ParserFactory
from models.model import Repository

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

@click.group()
def cli():
    pass


@cli.command()
@click.option('--delete-old-db', is_flag=True, help='Delete databse')
def init_db(delete_old_db):
    Repository.init_db(delete_old_db)


@cli.command()
@click.option('--path', prompt='dataset path', help='Path of dataset')
@click.option('--init-db', is_flag=True, help='init databse')
@click.option('--delete-old-db', is_flag=True, help='Delete databse')
def parse(path, init_db, delete_old_db):
    if init_db:
        Repository.init_db(delete_old_db)
    for file_name in os.listdir(path):
        file_path = os.path.join(path, file_name)
        parser = ParserFactory.get_parser(file_path)
        if parser:
            parser.parse(manager=Repository, limit=1000)
        else:
            logger.warn("couldn't find parser for %s", file_name)


@cli.command()
@click.option('--host', help='host')
@click.option('--port', help='port')
def api(host, port):
    from application import run_api
    run_api(host, port)


if __name__ == '__main__':
    cli()
