
from django.conf import settings
from django.db.backends.oracle.creation import DatabaseCreation as OracleDatabaseCreation
data_types = OracleDatabaseCreation.data_types
import sys
from datetime import datetime

def existing(settings_dict):
    """ Use existing database for tests?
        Cater for using existing Oracle databases both for format issues and
        for database tests where you dont have oracle dba drop database permissions
        Eg: if existing = 'Unicode' then its just used for the latter.
    """
    extras_setting = settings_dict['EXTRAS'] if 'EXTRAS' in settings_dict else {}

    if extras_setting.has_key('existing'):
        return extras_setting['existing']
    elif hasattr(settings, 'DATABASE_EXTRAS'):
        return settings.DATABASE_EXTRAS.get('existing','')
    else:
        return ''

# Uses the specified encoding
def get_ascii_data_types(settings_dict):
    if existing(settings_dict) == 'ASCII':
        # Default CharFields are not Unicode
        data_types['CharField'] = 'VARCHAR2(%(max_length)s)'
        data_types['FileField'] = 'VARCHAR2(%(max_length)s)'
        data_types['FilePathField'] = 'VARCHAR2(%(max_length)s)'
        return data_types
    else:
        return None

class DatabaseCreation(OracleDatabaseCreation):
    """ Allow for modification of the data types to cater for using
        older or existing Oracle database data.
    """
    start = datetime.now()

    def __init__(self, connection):
        OracleDatabaseCreation.__init__(self, connection)

        ascii_data_types = get_ascii_data_types(self.connection.settings_dict)
        if ascii_data_types:
            data_types = ascii_data_types

    def _create_test_db(self, verbosity=1, autoclobber=False):
        """ If existing is set then this uses the settings database
            for testing rather than creating a new one
        """
        self.start = datetime.now()

        # 'Option for using existing (non-production) database for tests'
        if existing(self.connection.settings_dict):
            conn_settings = self.connection.settings_dict
            if conn_settings.has_key('TEST_NAME'):
                if not conn_settings['TEST_NAME']:
                    conn_settings['TEST_NAME'] = conn_settings['NAME']
            # django pre 1.3 global settings
            elif hasattr(settings, 'TEST_DATABASE_NAME') and not settings.TEST_DATABASE_NAME:
                settings.TEST_DATABASE_NAME = settings.DATABASE_NAME
                settings.TEST_DATABASE_USER = settings.DATABASE_USER
                settings.TEST_DATABASE_PASSWD = settings.DATABASE_PASSWORD
        else:
            super(OracleDatabaseCreation, self)._create_test_db(verbosity=verbosity,
                autoclobber=autoclobber)
        test_db = conn_settings.get('TEST_NAME', getattr(settings, 'TEST_DATABASE_NAME', 'None'))
        print 'Using Test Database %s' % test_db


    def _destroy_test_db(self, test_database_name, verbosity=1):
        """ If existing is set then this must clean up all the test
            schema and data - not just drop the database
        """
        print "#### Built tables and tested in %s ####" % str(datetime.now() - self.start)
        if existing(self.connection.settings_dict):
            print 'Cleaning up test data and schema from %s' % settings.TEST_DATABASE_NAME
            self._drop_test_tables()
            self._delete_test_users()
        else:
            print 'Destroying Test Database %s' % settings.TEST_DATABASE_NAME
            super(OracleDatabaseCreation, self)._destroy_test_db(verbosity=verbosity,
                autoclobber=False)

    def list_test_tables(self, apps=None):
        """ Only used when using an existing database for testing.
            Retrieve the list of models that the tests create
            so they can be dropped again rather than blitzing the whole db
            NB: This assumes running the tests via the separate tests/manage.py
            Where all the apps are test apps - otherwise specify the test apps
        """
        from django.db import models
        test_tables = []
        tables = self.connection.introspection.table_names()
        if not apps:
            apps = models.get_apps()
        for app in apps:
            app_name = app.__name__.split('.')[-2]
            model_list = models.get_models(app)
            for model in model_list:
                table = self.connection.introspection.table_name_converter(model._meta.db_table)
                if table in tables:
                    test_tables.append(table)
        return test_tables

    def _drop_test_tables(self):
        """ Individually drop the test tables """
        from django.db import connection, transaction
        cursor = connection.cursor()
        try:
            for table in self.list_test_tables():
                statement = "drop table " + table + " cascade constraints purge"
                try:
                    cursor.execute(statement)
                except:
                    pass
                statement = "drop sequence " + table + "_SQ"
                try:
                    cursor.execute(statement)
                except:
                    pass
                print 'Deleted table and sequence %s' % table
            transaction.commit_unless_managed()
        except Exception, err:
            print 'Couldnt acquire transaction to delete test tables due to error: %s' % err
        return

    def _delete_test_data(self):
        """ Individually delete the test tables """
        from django.db import connection, transaction
        cursor = connection.cursor()
        try:
            for table in self.list_test_tables():
                statement = "delete from " + table
                try:
                    cursor.execute(statement)
                except:
                    pass
                try:
                    cursor.execute(statement)
                except:
                    pass
            print 'Deleted test data'
            transaction.commit_unless_managed()
        except Exception, err:
            print 'Couldnt acquire transaction to delete test data due to error: %s' % err
        return

    def _delete_test_users(self):
        """ individually delete the test users """
        user_clause =  " from auth_user where email like '%@example.com' or email is null"
        try:
            from django.db import connection, transaction
            cursor = connection.cursor()
            cursor.execute("delete from django_admin_log where user_id in (select id " + user_clause + ')')
            cursor.execute("delete " + user_clause)
            transaction.commit_unless_managed()
            print 'Deleted test users'
        except Exception, err:
            print 'Couldnt acquire connection to delete test users due to %s' % err
        return
