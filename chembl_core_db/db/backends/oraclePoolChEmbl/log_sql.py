# Middleware added if log level on INFO or DEBUG
# If dango DEBUG then write sql timings to screen else write to the log
from django.conf import settings
from django.db import connection
from django.template import Template, Context
from django.utils.html import strip_tags
logger = None
try:
    from chembl_core_db.db.backends.oracleChEmbl.base import get_extras
    from datetime import datetime
    if int(get_extras().get('log', 0)) == 10:
        import logging
        logger = logging.getLogger("oracle_pool")
except:
    pass

TMPL = Template('''
                <p>Session pool config: {{ pool }}</p>
                <p><em>Total query count:</em> {{ count }}<br/>
                <em>Total execution time:</em> {{ time }}</p>
                <ul class="sqllog">
                    {% for sql in sqllog %}
                        <li>{{ sql.time }}: {{ sql.sql }}</li>
                    {% endfor %}
                </ul>
                ''')

class SQLLogMiddleware:
    """ Writes the pool details and sql query timings either to the log
        or at bottom of the page if django DEBUG is set
    """

    def process_response ( self, request, response ):
        """ SQL logging to screen switched on by settings.DEBUG
            SQL logging to log dependent on database logger level
            ie. EXTRAS['log'] = 10
            Use case is need to turn on debug SQL logging on production installs
            occasionally - whilst DEBUG is only for development
        """
        if settings.DEBUG or logger:
            time = 0.0
            for q in connection.queries:
                time += float(q['time'])

            if settings.DEBUG:
                if response.__getitem__('Content-Type').startswith('text/html'):

                    content = TMPL.render(Context({'pool':self.pool(),
                                                   'sqllog':connection.queries,
                                                   'count':len(connection.queries),
                                                   'time':time}))

                    if response.content.find('</body>') == -1:
                        response.content += content
                    else:
                        try:
                            response.content = response.content.replace('</body>',
                                u"%s%s" % (content,'</body>'))
                        except:
                            pass

            if logger:
                logger.debug('%s SQL log for %s -----------------------' % (datetime.now(),
                                                                            request.path) )
                logger.debug(self.pool())
                logger.debug('Executed %s queries in %s' % (len(connection.queries),time))
                for query in connection.queries:
                    logger.debug(query)
        return response


    def pool(self):
        """ show pooling info """
        if hasattr(connection, 'get_config'):
            return str(connection.get_config())
        else:
            return 'No connection pooling implemented'
