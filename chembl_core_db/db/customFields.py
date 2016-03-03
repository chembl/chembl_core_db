__author__ = 'mnowotka'

from django.db import models
from django.db.models import Field
from django.db.models import NOT_PROVIDED
import base64
try:
    import cx_Oracle as oracle
except ImportError:
    oracle = None
try:
    import psycopg2
except ImportError:
    psycopg2 = None
from django.utils.translation import ugettext_lazy as _
from django.core import exceptions
from django.utils.datastructures import DictWrapper
from django.utils import six

#-----------------------------------------------------------------------------------------------------------------------

def _adjust_keywords(kwds):
    required = kwds.get('required', False)
    kwds['null'] = not required
    kwds['blank'] = not required
    if 'required' in kwds:
        del kwds['required']
    if 'choices' in kwds:
        kwds['choices'] = [(a, a) for a in kwds['choices']]
    return kwds

#-----------------------------------------------------------------------------------------------------------------------

class Blob(str):
    pass

#-----------------------------------------------------------------------------------------------------------------------

class BlobField(six.with_metaclass(models.SubfieldBase, models.TextField)):
    description = "Stores raw binary data"

    def __init__(self, *args, **kwds):
        kwds = _adjust_keywords(kwds)
        super(BlobField, self).__init__(*args, **kwds)

    def db_type(self, connection):
        if connection.vendor == 'mysql':
            return 'BLOB'
        if connection.vendor == 'oracle':
            return 'BLOB'
        if connection.vendor == 'postgresql':
            return 'bytea'
        if connection.vendor == 'sqlite':
            return 'BLOB'

    def get_internal_type(self):
        return "BlobField"

    def get_db_prep_value(self, value, connection=None, prepared=False):
        return value

    def get_db_prep_save(self, value, connection):
        if connection:
            if connection.vendor == 'oracle':
                curs = connection.cursor()
                blob = curs.var(oracle.BLOB)
                blob.setvalue(0, value)
                return blob
            if connection.vendor == 'postgresql':
                return psycopg2.Binary(value)
        return value

    def value_to_string(self, obj):
        return base64.b64encode(self._get_val_from_obj(obj))

    def to_python(self, value):
        if isinstance(value, Blob):
            return value
        if isinstance(value, basestring):
            try:
                unicode(value)
                return Blob(base64.b64decode(value))
            except UnicodeDecodeError:
                return Blob(value)
        return Blob(value)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblTextField(models.TextField):
    empty_strings_allowed = False

    def db_type(self, connection):
        if connection.vendor == 'oracle':
            return 'CLOB'
        return super(ChemblTextField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblCharField(models.CharField):
    empty_strings_allowed = False

    def __init__(self, *args, **kwargs):
        self.novalidate = kwargs.pop('novalidate', False)
        self.novalidate_default = kwargs.pop('novalidate_default', False)
        super(ChemblCharField, self).__init__(*args, **kwargs)


    def db_type(self, connection):

        data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
        default = ''
        if self.default != NOT_PROVIDED and not self.novalidate_default:
            default = (" DEFAULT '%s' " % str(self.default))

        if connection.vendor == 'postgresql':
            if self.max_length >= 2712:
                self.db_index = False
                self._unique = False
            type = 'varchar(%(max_length)s)'
            type += default
            if self.choices and not self.novalidate:
                choices = ', '.join(map(lambda x: "'%s'" % x[0].replace("'", "''"), self.choices))
                type += ' CHECK (%(column)s IN (' + choices + ')) '
            return (type % data)

        if connection.vendor == 'mysql':
            if self.max_length > 767:
                self.db_index = False
                self._unique = False
            type = 'varchar(%(max_length)s)'
            type += default
            return (type % data)

        if connection.vendor == 'sqlite':
            type = 'varchar(%(max_length)s)'
            type += default
            return (type % data)

        if connection.vendor == 'oracle':
            type = 'VARCHAR2(%(max_length)s BYTE)'
            type += default
            if self.choices and not self.novalidate:
                choices = ', '.join(map(lambda x: "'%s'" % x[0].replace("'", "''"), self.choices))
                type += ' CHECK (%(qn_column)s IN (' + choices + ')) '
            return (type % data)
        return super(ChemblCharField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblDateField(models.DateField):

    def db_type(self, connection):
        type = 'DATE'
        if connection.vendor == 'oracle':
            if self.default != NOT_PROVIDED:
                if hasattr(self.default, '__call__') and self.default.__name__ == 'today':
                    type += ' DEFAULT sysdate '
            return type
        if connection.vendor == 'postgresql':
            if self.default != NOT_PROVIDED:
                if hasattr(self.default, '__call__') and self.default.__name__ == 'today':
                    type += ' DEFAULT CURRENT_DATE '
            return type
        if connection.vendor == 'sqlite':
            if self.default != NOT_PROVIDED:
                if hasattr(self.default, '__call__') and self.default.__name__ == 'today':
                    type += " (datetime('now','localtime')) "
            return type
        return super(ChemblDateField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblNoLimitDecimalField(models.DecimalField):

    def __init__(self, verbose_name=None, name=None,  **kwargs):
        super(ChemblNoLimitDecimalField, self).__init__(verbose_name=verbose_name, name=name, max_digits=38,
            decimal_places=19, **kwargs)

    def db_type(self, connection):
        if connection.vendor == 'oracle':
            return 'NUMBER'
        if connection.vendor == 'postgresql':
            return 'NUMERIC'
        if connection.vendor == 'mysql':
            return 'NUMERIC(64,30)'
        return super(ChemblNoLimitDecimalField, self).db_type(connection)

    def format_number(self, value):
        return str(value)

    def get_db_prep_save(self, value, connection):
        if value is not None:
            return self.format_number(value)
        return value

#-----------------------------------------------------------------------------------------------------------------------

class ChemblPositiveDecimalField(models.DecimalField):

    def db_type(self, connection):
        if connection.vendor == 'oracle':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'NUMBER(%(max_digits)s, %(decimal_places)s) CHECK (%(qn_column)s >= 0)'
            return (type % data)

        if connection.vendor == 'postgresql':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type =  'numeric(%(max_digits)s, %(decimal_places)s) CHECK ("%(column)s" >= 0)'
            return (type % data)

        return super(ChemblPositiveDecimalField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblIntegerField(models.IntegerField):

    def get_internal_type(self):
        return "ChemblIntegerField"

    def __init__(self, length, *args, **kwargs):
        self.length = length
        super(ChemblIntegerField, self).__init__(*args, **kwargs)

    def db_type(self, connection):

        default = ''
        if self.default != NOT_PROVIDED:
            default = (' DEFAULT %s ' % str(int(self.default)))

        if connection.vendor == 'oracle':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'NUMBER(%s,0)' % (self.length)
            type += default
            if self.choices:
                choices = ', '.join(map(lambda x: str(x[0]), self.choices))
                type += ' CHECK (%(qn_column)s IN (' + choices + ')) '
            return (type % data)

        if connection.vendor == 'postgresql':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'integer'
            if self.length <= 4:
                type = 'smallint'
            if self.length > 9:
                type =  'bigint'
            type += default
            if self.choices:
                choices = ', '.join(map(lambda x: str(x[0]), self.choices))
                type += ' CHECK (%(column)s IN (' + choices + ')) '
            return (type % data)

        if connection.vendor == 'mysql':
            type = 'int(%s)' % (self.length)
            return type + default

        if connection.vendor == 'sqlite':
            type = 'integer'
            if self.length <= 4:
                type = 'smallint'
            if self.length > 9:
                type =  'bigint'
            return type + default

        return super(ChemblIntegerField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblNullBooleanField(models.IntegerField):

    def get_internal_type(self):
        return "ChemblNullBooleanField"

    def db_type(self, connection):
        default = ''
        if self.default != NOT_PROVIDED:
            default = (' DEFAULT %s ' % str(int(self.default)))
        if connection.vendor == 'oracle':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'NUMBER(1,0)'
            type += default
            type += ' CHECK (%(qn_column)s IN (0,1,-1)) '
            return (type % data)
        if connection.vendor == 'postgresql':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'smallint'
            type += default
            type += ' CHECK ("%(column)s" in (0,1,-1))'
            return (type % data)
        if connection.vendor == 'mysql':
            type = 'int(1)'
            type += default
            return type
        if connection.vendor == 'sqlite':
            type = 'tinyint'
            type += default
            return type
        return super(ChemblNullBooleanField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblNullableBooleanField(models.NullBooleanField):

    def __init__(self, *args, **kwargs):
        kwargs['null'] = True
        kwargs['blank'] = True
        Field.__init__(self, *args, **kwargs)
        if self.default != NOT_PROVIDED:
            self.blank = False # blank is false because it has default value

    def get_db_prep_value(self, value, connection=None, prepared=False):
        if connection.vendor == 'postgresql':
            if value is None:
                return None
            if value:
                return 1
            return 0
        return super(ChemblNullableBooleanField, self).get_db_prep_value(value, connection, prepared)

    def db_type(self, connection):
        if connection.vendor == 'oracle':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'NUMBER(1)'
            if self.default != NOT_PROVIDED:
                type += (' DEFAULT %s ' % str(int(self.default)))
            type += ' CHECK ((%(qn_column)s IN (0,1)) OR (%(qn_column)s IS NULL)) '
            return (type % data)
        if connection.vendor == 'postgresql':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'smallint'
            if self.default != NOT_PROVIDED:
                type += (' DEFAULT %s ' % str(int(self.default)))
            type += ' CHECK ((%(column)s IN (0,1)) OR (%(column)s IS NULL)) '
            return (type % data)
        if connection.vendor == 'mysql':
            type = 'bool'
            if self.default != NOT_PROVIDED:
                type += (' DEFAULT %s ' % str(int(self.default)))
            return type
        if connection.vendor == 'sqlite':
            type = 'BOOL'
            if self.default != NOT_PROVIDED:
                type += (' DEFAULT %s ' % str(int(self.default)))
            return type
        return super(ChemblNullableBooleanField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblBooleanField(models.BooleanField):

    def __init__(self, *args, **kwargs):
        Field.__init__(self, *args, **kwargs)

    def get_db_prep_value(self, value, connection=None, prepared=False):
        if connection.vendor == 'postgresql':
            if value:
                return 1
            return 0
        return super(ChemblBooleanField, self).get_db_prep_value(value, connection, prepared)

    def db_type(self, connection):
        if connection.vendor == 'oracle':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'NUMBER(1)'
            if self.default != NOT_PROVIDED:
                type += (' DEFAULT %s ' % str(int(self.default)))
            type += ' CHECK (%(qn_column)s IN (0,1)) '
            return (type % data)
        if connection.vendor == 'postgresql':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'smallint'
            if self.default != NOT_PROVIDED:
                type += (' DEFAULT %s ' % str(int(self.default)))
            type += ' CHECK (%(column)s IN (0,1)) '
            return (type % data)
        if connection.vendor == 'mysql':
            type = 'bool'
            if self.default != NOT_PROVIDED:
                type += (' DEFAULT %s ' % str(int(self.default)))
            return type
        if connection.vendor == 'sqlite':
            type = 'BOOL'
            if self.default != NOT_PROVIDED:
                type += (' DEFAULT %s ' % str(int(self.default)))
            return type
        return super(ChemblBooleanField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblPositiveIntegerField(models.IntegerField):

    def get_internal_type(self):
        return "PositiveIntegerField"

    def formfield(self, **kwargs):
        defaults = {'min_value': 0}
        defaults.update(kwargs)
        return super(ChemblPositiveIntegerField, self).formfield(**defaults)

    def __init__(self, length, *args, **kwargs):
        self.length = length
        super(ChemblPositiveIntegerField, self).__init__(*args, **kwargs)

    def db_type(self, connection):

        default = ''
        choices = ''

        if self.default != NOT_PROVIDED:
            default = (' DEFAULT %s ' % str(int(self.default)))

        if connection.vendor == 'oracle':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'NUMBER(%s,0)' % (self.length)
            type += default
            if self.choices:
                choices = ' AND %(qn_column)s IN ('+ ', '.join(map(lambda x: str(x[0]), self.choices)) + ' ) '
            type += ' CHECK (%(qn_column)s >= 0' + choices + ') '
            return (type % data)

        if connection.vendor == 'postgresql':
            data = DictWrapper(self.__dict__, connection.ops.quote_name, "qn_")
            type = 'integer'
            if self.length <= 4:
                type =  'smallint'
            if self.length > 9:
                type = 'bigint'
            type += default
            if self.choices:
                choices = ' AND %(column)s IN ('+ ', '.join(map(lambda x: str(x[0]), self.choices)) + ' ) '
            type += ' CHECK (%(column)s >= 0' + choices + ') '
            return (type % data)

        if connection.vendor == 'mysql':
            type =  'int(%s) UNSIGNED' % (self.length)
            type += default
            return type

        if connection.vendor == 'sqlite':
            type = 'integer'
            if self.length <= 4:
                type = 'smallint'
            if self.length > 9:
                type =  'bigint'
            type += ' unsigned '
            type += default
            return type

        return super(ChemblPositiveIntegerField, self).db_type(connection)

#-----------------------------------------------------------------------------------------------------------------------

class ChemblAutoField(Field):
    description = _("Integer")

    empty_strings_allowed = False
    default_error_messages = {
        'invalid': _(u"'%s' value must be an integer."),
        }

    def __init__(self, length, *args, **kwargs):
        assert kwargs.get('primary_key', False) is True,\
        "%ss must have primary_key=True." % self.__class__.__name__
        kwargs['blank'] = True
        self.length = length
        Field.__init__(self, *args, **kwargs)

    def get_internal_type(self):
        return "AutoField"

    def db_type(self, connection):
        if connection.vendor == 'oracle':
            return 'NUMBER(%s,0)' % (self.length)
        if connection.vendor == 'postgresql':
            if self.length <= 4:
                return 'smallint'
            if self.length <= 9:
                return 'integer'
            return 'bigint'
        if connection.vendor == 'mysql':
            return 'int(%s)' % (self.length)
        if connection.vendor == 'sqlite':
            type = 'integer'
            if self.length <= 4:
                type = 'smallint'
            if self.length > 9:
                type =  'bigint'
            return type
        return super(ChemblAutoField, self).db_type(connection)

    def to_python(self, value):
        if value is None:
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            msg = self.error_messages['invalid'] % str(value)
            raise exceptions.ValidationError(msg)

    def validate(self, value, model_instance):
        pass

    def get_prep_value(self, value):
        if value is None:
            return None
        return int(value)

    def contribute_to_class(self, cls, name):
        assert not cls._meta.has_auto_field,\
        "A model can't have more than one AutoField."
        super(ChemblAutoField, self).contribute_to_class(cls, name)
        cls._meta.has_auto_field = True
        cls._meta.auto_field = self

    def formfield(self, **kwargs):
        return None

#-----------------------------------------------------------------------------------------------------------------------