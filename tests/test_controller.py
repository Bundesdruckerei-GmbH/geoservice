import pytest
from geoservice.model.geoobject import Adm0, Adm1


@pytest.mark.skip
def test_api_adm0(app, db):
    # -----------------------------------------------------------------
    # GIVEN
    adm0_usa = Adm0(adm0_code="USA", name="United States of America")
    adm0_deu = Adm0(adm0_code="DEU", name="Bundesrepublik Deutschland")
    
    db.session.add(adm0_usa)
    db.session.add(adm0_deu)
    db.session.commit()
    # -----------------------------------------------------------------
    # WHEN
    result = app.get("/api/geo/adm0/")
    # -----------------------------------------------------------------
    # THEN
    assert result.json == {
        "items": [
            {
                'adm0_code': 'DEU',
                'geometry_level': 0,
                'name': 'Bundesrepublik Deutschland',
            },
            {
                'adm0_code': 'USA',
                'geometry_level': 0,
                'name': 'United States of America',
            },
        ]
    }


@pytest.mark.skip
def test_api_adm1(app, db):
    # -----------------------------------------------------------------
    # GIVEN
    adm0_nrw = Adm1(adm0_code="DEU", adm1_code="NRW", name="Nordrhein-Westfalen")
    adm0_bay = Adm1(adm0_code="DEU", adm1_code="BAY", name="Bayern")

    db.session.add(adm0_nrw)
    db.session.add(adm0_bay)
    db.session.commit()
    # -----------------------------------------------------------------
    # WHEN
    result = app.get("/api/geo/adm1/")
    # -----------------------------------------------------------------
    # THEN
    assert result.json == {
        "items": [
            {
                'adm0_code': 'DEU',
                'adm1_code': 'BAY',
                'geometry_level': 0,
                'name': 'Bayern',
            },
            {
                'adm0_code': 'DEU',
                'adm1_code': 'NRW',
                'geometry_level': 0,
                'name': 'Nordrhein-Westfalen',
            },
        ]
    }
