"""
Oracle database backend for Django.

Requires cx_Oracle: http://cx-oracle.sourceforge.net/
"""

from chembl_core_db.db.backends.oracleChEmbl.base import DatabaseOperations as ChemDatabaseOperations
from chembl_core_db.db.backends.oracleChEmbl.base import DatabaseWrapper as OracleDatabaseWrapper
from chembl_core_db.db.backends.oracleChEmbl.base import DatabaseFeatures as OracleDatabaseFeatures
from introspection import DatabaseIntrospection

import os
import thread
import logging
from django.conf import settings
from chembl_core_db.db.backends.oracleChEmbl.base import FormatStylePlaceholderCursor as OracleFormatStylePlaceholderCursor
from chembl_core_db.timeLimited import TimeLimitExpired

try:
    from django.db.backends.signals import connection_created
except:
    connection_created = None
try:
    import cx_Oracle as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading cx_Oracle module: %s" % e)

from django.utils.encoding import force_bytes
convert_unicode = force_bytes

def get_extras(user_defined_extras):
    """ Oracle already has OPTIONS specific to cx_Oracle.connection() use
        This adds extra pool and sql logging attributes to the settings

        'homogeneous':1, # 1 = single credentials, 0 = multiple credentials
        Dropped this option to use multiple credentials since if supplied
        to Database.version (ie cx_Oracle) < '5.0.0' it breaks and we want
        separate pools for separate credentials anyhow.
    """
    default_extras = {'min':4,         # start number of connections
                      'max':8,         # max number of connections
                      'increment':1,   # increase by this amount when more are needed
                      'threaded':True, # server platform optimisation
                      'timeout':600,   # connection timeout, 600 = 10 mins
                      'log':0,         # extra logging functionality
                      'logpath':'',    # file system path for oraclepool.log file
                      'existing':'',   # Type modifications if using existing database data
                      'like':'LIKEC',  # Use LIKE or LIKEC - Oracle ignores index for LIKEC on older dbs
                      'session':[]     # Add session optimisations applied to each fresh connection, eg.
                      #   ['alter session set cursor_sharing = similar',
                      #    'alter session set session_cached_cursors = 20']
    }

    if user_defined_extras and len(user_defined_extras) != 0:
        return user_defined_extras
    elif hasattr(settings, 'DATABASE_EXTRAS'):
        return settings.DATABASE_EXTRAS
    else:
        return default_extras

def get_logger(extras):
    """ Check whether logging is required
        If log level is more than zero then logging is performed
        If log level is DEBUG then logging is printed to screen
        If no logfile is specified then unless its DEBUG to screen its added here
        NB: Log levels are 10 DEBUG, 20 INFO, 30 WARNING, 40 ERROR, 50 CRITICAL
    """

    loglevel = int(extras.get('log', 0))
    if loglevel > 0:
        import logging
        logfile = extras.get('logpath','')
        if logfile.endswith('.log'):
            (logfile, filename) = os.path.split(logfile)
        else:
            filename = 'oraclepool.log'
        if os.path.exists(logfile):
            logfile = os.path.join(logfile, filename)
        else:
            logfile = ''
        if not logfile and extras.get('log') > logging.DEBUG:
            logfile = '.'
        if logfile in ['.', '..']:
            logfile = os.path.join(os.path.abspath(os.path.dirname(logfile)), filename)
            # if log file is writable do it
        if not logfile:
            raise Exception('Log path %s not found' % extras.get('logpath', ''))
        else:
            logging.basicConfig(filename=logfile, level=loglevel)
            mylogger = logging.getLogger(__name__)
            mylogger.setLevel(loglevel)
            chandler = logging.StreamHandler()
            chandler.setLevel(loglevel)
            fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            formatter = logging.Formatter(fmt)
            chandler.setFormatter(formatter)
            mylogger.addHandler(chandler)
            from datetime import datetime
            msg = '''%s #### Started django-oraclepool
                     SQL logging at level %s ####''' % (datetime.now(), loglevel)
            mylogger.info(msg)
            return mylogger
    else:
        # 'No logging set'
        return None


class DatabaseFeatures(OracleDatabaseFeatures):
    """ Add extra options from default Oracle ones
        Plus switch off save points and id return
        See
        http://groups.google.com/group/django-developers/browse_thread/thread/bca33ecf27ff5d63
        Savepoints could be turned on but are not needed
        and since they may impact performance they are turned off here
    """
    uses_savepoints = False
    allows_group_by_ordinal = False
    supports_tablespaces = True
    uses_case_insensitive_names = True
    time_field_needs_date = True
    date_field_supports_time_value = False

class DatabaseWrapper(OracleDatabaseWrapper):

    poolprops = {'homogeneous':'',
                 'increment':'',
                 'max':'',
                 'min':'',
                 'busy':'',
                 'opened':'',
                 'name':'',
                 'timeout':'',
                 'tnsentry':''
    }

    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': "LIKEC %s ESCAPE '\\'",
        'icontains': "LIKEC UPPER(%s) ESCAPE '\\'",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKEC %s ESCAPE '\\'",
        'endswith': "LIKEC %s ESCAPE '\\'",
        'istartswith': "LIKEC UPPER(%s) ESCAPE '\\'",
        'iendswith': "LIKEC UPPER(%s) ESCAPE '\\'",
        }


    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        user_defined_extras = self.settings_dict['EXTRAS'] if 'EXTRAS' in self.settings_dict else {}
        self.extras = get_extras(user_defined_extras)
        self.logger = get_logger(self.extras)

        if self.extras.get('like', 'LIKEC') != 'LIKEC':
            for key in ['contains',
                        'icontains',
                        'startswith',
                        'istartswith',
                        'endswith',
                        'iendswith']:
                self.operators[key] = self.operators[key].replace('LIKEC',
                    self.extras['like'])

        self.features = DatabaseFeatures(self)
        self.ops = ChemDatabaseOperations(self)
        self.introspection = DatabaseIntrospection(self)

    def get_config(self):
        """ Report the oracle connection and pool data see
            http://cx-oracle.sourceforge.net/html/session_pool.html#sesspool
        """
        pool = self._get_pool()
        if pool:
            for key in self.poolprops.keys():
                try:
                    self.poolprops[key] = getattr(pool, key, '')
                except:
                    pass
        else:
            self.poolprops['name'] = 'Session pool not found'
        return self.poolprops

    def _get_pool (self):
        """ Get the connection pool or create it if it doesnt exist
            Add thread lock to prevent server initial heavy load creating multiple pools
        """
        pool_name = '_pool_%s' % getattr(self, 'alias', 'common')
        if not hasattr (self.__class__, pool_name):
            lock = thread.allocate_lock()
            lock.acquire()
            if not hasattr (self.__class__, pool_name):
                if self.extras['threaded']:
                    Database.OPT_Threading = 1
                else:
                    Database.OPT_Threading = 0
                    # Use 1.2 style dict if its there, else make one
                try:
                    settings_dict = self.creation.connection.settings_dict
                except:
                    settings_dict = None

                if not settings_dict.get('NAME',''):
                    settings_dict = {'HOST':settings.DATABASE_HOST,
                                     'PORT':settings.DATABASE_PORT,
                                     'NAME':settings.DATABASE_NAME,
                                     'USER':settings.DATABASE_USER,
                                     'PASSWORD':settings.DATABASE_PASSWORD,
                                     'EXECUTION_TIMEOUT' : '',
                                     }
                if len(settings_dict.get('HOST','').strip()) == 0:
                    settings_dict['HOST'] = 'localhost'
                if len(settings_dict.get('PORT','').strip()) != 0:
                    dsn = Database.makedsn(str(settings_dict['HOST']),
                        int(settings_dict['PORT']),
                        str(settings_dict.get('NAME','')))
                else:
                    dsn = settings_dict.get('NAME','')

                timeout = settings_dict.get('EXECUTION_TIMEOUT','').strip()
                if timeout:
                    timeout = int(timeout)
                else:
                    timeout = None

                try:
                    pool = Database.SessionPool(str(settings_dict.get('USER','')),
                        str(settings_dict.get('PASSWORD','')),
                        dsn,
                        int(self.extras.get('min', 4)),
                        int(self.extras.get('max', 8)),
                        int(self.extras.get('increment', 1)),
                        threaded = self.extras.get('threaded',
                            True))
                except Exception, err:
                    pool = None
                if pool:
                    if self.extras.get('timeout', 0):
                        pool.timeout = self.extras['timeout']
                    setattr(self.__class__, pool_name, pool)
                else:
                    msg = """##### Database '%(NAME)s' login failed or database not found #####
                             Using settings: %(USER)s @ %(HOST)s:%(PORT)s / %(NAME)s
                             Django start up cancelled
                          """ % settings_dict
                    msg += '\n##### DUE TO ERROR: %s\n' % err
                    log = logging.getLogger(__name__)
                    log.critical(msg)
                    return None
                lock.release()
        return getattr(self.__class__, pool_name)

    pool = property(_get_pool)

    def _valid_connection(self):
        return self.connection is not None

    def _connect_string(self):
        settings_dict = self.settings_dict
        if not settings_dict['HOST'].strip():
            settings_dict['HOST'] = 'localhost'
        if settings_dict['PORT'].strip():
            dsn = Database.makedsn(settings_dict['HOST'],
                int(settings_dict['PORT']),
                settings_dict['NAME'])
        else:
            dsn = settings_dict['NAME']
        return "%s/%s@%s" % (settings_dict['USER'],
                             settings_dict['PASSWORD'], dsn)

    def _cursor(self, settings=None):
        """ Get a cursor from the connection pool """
        cursor = None
        timeout =  self.settings_dict.get('EXECUTION_TIMEOUT', '').strip()
        if timeout:
            timeout = int(timeout)
            self.ping(timeout)
        else:
            timeout = None

        if self.pool is not None:
            if self.connection is None:

                # Get a connection, after confirming that is a valid connection
                self.connection = self._get_alive_connection()

                if connection_created:
                    # Assume acquisition of existing connection = create for django signal
                    connection_created.send(sender=self.__class__)
                if self.logger:
                    self.logger.info("Acquire pooled connection \n%s\n" % self.connection.dsn)

                cursor = FormatStylePlaceholderCursor(self.connection, self.logger, timeout)

                # In case one connection in the pool dies we need to retry others in the pool
                retry = 0
                max_retry = self.extras.get('min',4)
                while (retry < max_retry):
                    try:
                        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD' "
                                       "NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'")
                        retry = max_retry
                    except Database.Error, error:
                        if self.logger:
                            self.logger.warn("Failed to set session date due to error: %s" % error)
                            # If we have exhausted all of our connections in our pool raise the error
                        if retry == max_retry - 1:
                            if self.logger:
                                self.logger.critical("Exhausted %d connections in the connection pool")
                            raise

                    retry += 1

                if self.extras.get('session', []):
                    for sql in self.extras['session']:
                        cursor.execute(sql)

                try:
                    # There's no way for the DatabaseOperations class to know the
                    # currently active Oracle version, so we do some setups here.
                    # TODO: Multi-db support will need a better solution (a way to
                    # communicate the current version).
                    self.oracle_version = int(self.connection.version.split('.')[0])

                    if self.oracle_version <= 9:
                        self.ops.regex_lookup = self.ops.regex_lookup_9
                    else:
                        self.ops.regex_lookup = self.ops.regex_lookup_10
                except ValueError, err:
                    if self.logger:
                        self.logger.warn(str(err))

                try:
                    self.connection.stmtcachesize = 20
                except:
                    # Django docs specify cx_Oracle version 4.3.1 or higher, but
                    # stmtcachesize is available only in 4.3.2 and up.
                    pass
            else:
                cursor = FormatStylePlaceholderCursor(self.connection, self.logger, timeout)
        else:
            if self.logger:
                self.logger.critical('Pool couldnt be created - please check your Oracle connection or credentials')
            else:
                raise Exception('Pool couldnt be created - please check your Oracle connection or credentials')

        if not cursor:
            cursor = FormatStylePlaceholderCursor(self.connection, self.logger, timeout)
            # Default arraysize of 1 is highly sub-optimal.
        cursor.arraysize = 100
        return cursor

    def _get_alive_connection(self):
        """ Get a connection from the connection pool.  Make sure it's a valid connection (using ping()) before returning it. """
        connection_ok = False
        sanity_check = 0
        sanity_threshold = self.extras.get('max',10)

        while connection_ok == False:
            new_conn = self.pool.acquire()
            try:
                new_conn.ping()
                connection_ok = True
            except Database.Error, error:
                sanity_check += 1
                if sanity_check > sanity_threshold:
                    raise Exception('Could not get a valid/alive connection from the connection pool.')
                if self.logger:
                    self.logger.critical('Found a dead connection.  Dropping from pool.')
                self.pool.drop(new_conn)

        return new_conn

    def close(self):
        """ Releases connection back to pool """
        if self.connection is not None:
            if self.logger:
                self.logger.debug("Release pooled connection\n%s\n" % self.connection.dsn)
            try:
                self.pool.release(self.connection)
            except Database.OperationalError, error:
                if self.logger:
                    self.logger.debug("Release pooled connection failed due to: %s" % str(error))
            finally:
                self.connection = None

    def _savepoint_commit(self, sid):
        """ Oracle doesn't support savepoint commits.  Ignore them. """
        pass

    def _rollback(self):
        if self.connection:
            try:
                self.connection.rollback()
            except Database.OperationalError, error:
                if self.logger:
                    self.logger.debug("Rollback failed due to:  %s" % str(error))

class FormatStylePlaceholderCursor(OracleFormatStylePlaceholderCursor):
    """ Added just to allow use of % for like queries without params
        and use of logger if present.
    """

    def __init__(self, connection, logger, timeout = None):
        OracleFormatStylePlaceholderCursor.__init__(self, connection)
        self.logger = logger
        self.timeout = timeout

    def cleanquery(self, query, args=None):
        """ cx_Oracle wants no trailing ';' for SQL statements.  For PL/SQL, it
            it does want a trailing ';' but not a trailing '/'.  However, these
            characters must be included in the original query in case the query
            is being passed to SQL*Plus.

            Split out this as a function and allowed for no args so
            % signs can be used in the query without requiring parameterization
        """
        #        if query.find('INSERT')> -1:
        #            raise Exception(query) #params[8])
        if query.endswith(';') or query.endswith('/'):
            query = query[:-1]
        if not args:
            return convert_unicode(query, self.charset)
        else:
            try:
                return convert_unicode(query % tuple(args), self.charset)
            except TypeError, error:
                err = 'Parameter parsing failed due to error %s for query: %s' % (error,
                                                                                  query)
                if self.logger:
                    self.logger.critical(err)
                else:
                    raise Exception(err)

    def execute(self, query, params=[]):
        if params is None:
            args = None
        else:
            params = self._format_params(params)
            args = [(':arg%d' % i) for i in range(len(params))]
        query = self.cleanquery(query, args)
        self._guess_input_sizes([params])
        try:
            return self.cursor.execute(query, self._param_generator(params))
        except Database.Error, error:
            # cx_Oracle <= 4.4.0 wrongly raises a Database.Error for ORA-01400.
            if error.args[0].code == 1400 and not isinstance(error,
                Database.IntegrityError):
                error = Database.IntegrityError(error.args[0])
            err = '%s due to query:%s' % (error, query)
            if self.logger:
                self.logger.critical(err)
            else:
                raise Exception(err)


    def executemany(self, query, params=[]):
        try:
            args = [(':arg%d' % i) for i in range(len(params[0]))]
        except (IndexError, TypeError):
            # No params given, nothing to do
            return None
        query = self.cleanquery(query, args)
        formatted = [self._format_params(i) for i in params]
        self._guess_input_sizes(formatted)
        try:
            return (self.cursor.executemany, query,
                [self._param_generator(p) for p in formatted])
        except Database.Error, error:
            # cx_Oracle <= 4.4.0 wrongly raises a Database.Error for ORA-01400.
            if error.args[0].code == 1400 and not isinstance(error,
                Database.IntegrityError):
                error = Database.IntegrityError(error.args[0])
            if self.logger:
                self.logger.critical('%s due to query: %s' % (error, query))
            else:
                raise
