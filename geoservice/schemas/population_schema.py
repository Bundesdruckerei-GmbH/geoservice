from marshmallow import Schema, fields, validates_schema, ValidationError

from sqlalchemy import text, bindparam

from ..model.base import db


def check_years_limit(source = "WPP2022") -> list:
    """
    Check min and max of years in population data
    """
    
    query = "SELECT MIN(year), MAX(year) FROM population WHERE source = :source;"

    ret_val = db.session.execute(text(query).bindparams(
                bindparam('source', value=source)))
                    
    return ret_val.all()

def query_population(adm0_codes:list, years:list, source = "WPP2022") -> list:
    """
    Get population data from dataset
    """

    if (len(adm0_codes) == 0) & (len(years) == 0):
        query = """
            SELECT ARRAY_TO_JSON(ARRAY_AGG(sel))
            FROM (SELECT adm0_code, value, year
                FROM population
                WHERE source = :source) 
                AS sel;
        """

        ret_val = db.session.execute(text(query).bindparams(
                bindparam('source', value=source)))
        
        return ret_val.all()
    
    if (len(adm0_codes) == 0) & (len(years) != 0):
        query = """
            SELECT ARRAY_TO_JSON(ARRAY_AGG(sel))
            FROM (SELECT adm0_code, value, year
                FROM population
                WHERE year IN :years AND source = :source) 
                AS sel;
        """

        ret_val = db.session.execute(text(query).bindparams(
                bindparam('years', value = years, expanding=True),
                bindparam('source', value=source)))
                        
        return ret_val.all()
    
    if (len(adm0_codes) != 0) & (len(years) == 0):
        query = """
            SELECT ARRAY_TO_JSON(ARRAY_AGG(sel))
            FROM (SELECT adm0_code, value, year
                FROM population
                WHERE adm0_code IN :adm0_codes AND source = :source) 
                AS sel;
        """

        ret_val = db.session.execute(text(query).bindparams(
                bindparam('adm0_codes', value = adm0_codes, expanding=True),
                bindparam('source', value=source)))
                        
        return ret_val.all()

    query = """
        SELECT ARRAY_TO_JSON(ARRAY_AGG(sel))
        FROM (SELECT adm0_code, value, year
            FROM population
            WHERE adm0_code IN :adm0_codes AND year IN :years AND source = :source) 
            AS sel;
    """

    ret_val = db.session.execute(text(query).bindparams(
            bindparam('adm0_codes', value = adm0_codes, expanding=True),
            bindparam('years', value = years, expanding=True),
            bindparam('source', value=source)))
                    
    return ret_val.all()


class PopulationParameterSchema(Schema):
    filter_aerial_code = fields.List(fields.Str(), metadata={"description": "For which countries (by code) population values are selected"})
    years = fields.List(fields.Int(), metadata={"description": "For which specific years population values are selected"})
    years_from = fields.Int(metadata={"description": "From which year on population values are selected (inclusive)"})
    years_to = fields.Int(metadata={"description": "Until which year population values are selected (inclusive)"})
    source = fields.Str(load_default = 'WPP2022')

    @validates_schema
    def validate_method(self, args, **kwargs):
        if "" == args.get('source',""):
            raise ValidationError(f"source required for API call")
        if (args.get('years_from', None)!= None) & (args.get('years_to', None)!= None):
            if args.get('years_to', 1) < args.get('years_from', 0):
                raise ValidationError(f"years_from must be larger or equal to years_to")
        

    @classmethod
    def fetch(cls,args):
        years = args.get('years', [])
        if (args.get('years_from', None)!= None) & (args.get('years_to', None)!= None):
            new_years = list(range(args.get('years_from', None),args.get('years_to', None) + 1))
            years.extend(new_years)
            years = list(set(years))
        
        if (args.get('years_from', None)!= None) & (args.get('years_to', None)== None):
            new_years = list(range(args.get('years_from', None),check_years_limit(args.get('source', ""))[0][1] + 1))
            years.extend(new_years)
            years = list(set(years))
        
        if (args.get('years_from', None)== None) & (args.get('years_to', None)!= None):
            new_years = list(range(check_years_limit(args.get('source', ""))[0][0],args.get('years_to', None) + 1))
            years.extend(new_years)
            years = list(set(years))

        return query_population(args.get('filter_aerial_code', []),
                         years,
                         args.get('source', ""))[0][0]
