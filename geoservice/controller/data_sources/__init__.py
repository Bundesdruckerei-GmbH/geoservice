import importlib
from os.path import dirname, basename, isfile, join
import glob

modules = glob.glob(join(dirname(__file__), "*.py"))
module_names = (
    basename(f)[:-3]
    for f in modules
    if isfile(f) and not f.endswith('__init__.py')
)
# - - - - - - - - - - - - - - - - - - - -
for module_name in module_names:
    importlib.import_module(f'.{module_name}', package='geoservice.controller.data_sources')
# - - - - - - - - - - - - - - - - - - - -
__all__ = [module_names]
