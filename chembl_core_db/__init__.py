__author__ = 'mnowotka'

try:
    __version__ = __import__('pkg_resources').get_distribution('chembl_core_db').version
except Exception as e:
    __version__ = 'development'

from django.apps import AppConfig

class ChEMBLCoreDBConfig(AppConfig):
    name = 'chembl_core_db'

    def ready(self):
        from chembl_core_db.db.models.lookups import *

default_app_config = 'chembl_core_db.ChEMBLCoreDBConfig'

