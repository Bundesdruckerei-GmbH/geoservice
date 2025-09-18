import pytest

from geoservice import model
import geoservice


@pytest.fixture(autouse=False, scope='session')
def app():
    """a testable instance of the application

    see https://flask.palletsprojects.com/en/2.1.x/testing/
    """
    with geoservice.app.app_context():
        yield geoservice.app.test_client()


@pytest.fixture(autouse=False, scope='session')
def db():
    model.db.create_all()
    yield model.db
    model.db.drop_all()
