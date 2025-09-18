from pathlib import Path
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

from ..application import app


db = SQLAlchemy(app)
migrate = Migrate(app, db, directory=str(Path(__file__).parent / 'migrations'))


class Base(db.Model):
    __abstract__ = True

    id = db.Column(db.Integer, primary_key=True)


def commit():
    db.session.commit()
