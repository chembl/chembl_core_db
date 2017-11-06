from django.db.models.lookups import IContains, BuiltinLookup
from chembl_core_db.db.customFields import ChemblIndexedCharField
from django.db.models.expressions import Value


class OracleIContains(IContains):
    def process_lhs(self, compiler, connection, lhs=None):
        lhs_sql, params = super(BuiltinLookup, self).process_lhs(
            compiler, connection, lhs)
        field_internal_type = self.lhs.output_field.get_internal_type()
        db_type = self.lhs.output_field.db_type(connection=connection)
        lhs_sql = connection.ops.field_cast_sql(
            db_type, field_internal_type) % lhs_sql
        return lhs_sql, list(params)

    def process_rhs(self, compiler, connection):
        value = '{' + self.rhs + '}'
        if self.bilateral_transforms:
            if self.rhs_is_direct_value():
                # Do not call get_db_prep_lookup here as the value will be
                # transformed before being used for lookup
                value = Value(value, output_field=self.lhs.output_field)
            value = self.apply_bilateral_transforms(value)
            value = value.resolve_expression(compiler.query)
        # Due to historical reasons there are a couple of different
        # ways to produce sql here. get_compiler is likely a Query
        # instance, _as_sql QuerySet and as_sql just something with
        # as_sql. Finally the value can of course be just plain
        # Python value.
        if hasattr(value, 'get_compiler'):
            value = value.get_compiler(connection=connection)
        if hasattr(value, 'as_sql'):
            sql, params = compiler.compile(value)
            return '(' + sql + ')', params
        if hasattr(value, '_as_sql'):
            sql, params = value._as_sql(connection=connection)
            return '(' + sql + ')', params
        else:
            return self.get_db_prep_lookup(value, connection)

    def as_oracle(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        params = lhs_params + rhs_params
        return 'CONTAINS(%s, %s) > 0' % (lhs, rhs), params


ChemblIndexedCharField.register_lookup(OracleIContains)