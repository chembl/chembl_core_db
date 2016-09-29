__author__ = 'mnowotka'

from django.db import models
from django.db import connections
from django.db.models.query import QuerySet
from django.conf import settings

#-----------------------------------------------------------------------------------------------------------------------

class CompoundMolsMixin(object):

    def get_column(self, name):
        return filter(lambda x: x.name == name, self.model._meta.fields)[0].db_column or name

    def similar_to(self, structure, similarity_index, **kwargs):
        ctab_column = kwargs.get('ctab_column', self.get_column('ctab'))
        molregno_column = kwargs.get('molecule_column', self.get_column('molecule'))
        try:
            sim = int(similarity_index)
        except ValueError:
            raise ValueError('similarity_index must be integer from range (50,100)')
        if sim < 50 or sim > 100:
            raise ValueError('similarity_index must be integer from range (50,100)')

        connection = connections[self._db or 'default']
        if connection.vendor == 'oracle':
            return self.extra(select={'similarity': "TO_NUMBER (molsim (" + ctab_column + ",%s, 'normal'))"},
                select_params=(structure,),
                where=["molsim (" + ctab_column + ", %s, 'normal') BETWEEN %s AND '100'"],
                params=('smiles:' + structure, similarity_index))
        if connection.vendor == 'postgresql':
            fingerprints_table = kwargs.get('fingerprints_table', getattr(settings, 'FINGERPRINTS_TABLE', 'fps_rdkit'))
            fingerprints_pk = kwargs.get('fingerprints_pk', getattr(settings, 'FINGERPRINTS_PK', 'molregno'))
            similarity_type = kwargs.get('similarity_type', getattr(settings, 'SIMILARITY_TYPE', 'tanimoto_sml'))
            fingerprint_type = kwargs.get('fingerprint_type', getattr(settings, 'FINGERPRINT_TYPE', 'morganbv_fp'))
            fingerprint_column = kwargs.get('fingerprint_column', getattr(settings, 'FINGERPRINT_COLUMN', 'mfp2'))
            cursor = connection.cursor()
            cursor.execute("select 'c1ccccc1O'::mol;") # dirty hack but what can I do...
            cursor.execute('set rdkit.tanimoto_threshold=%s;', (sim / 100.0,))
            ret = self.extra(
                select={'similarity': similarity_type + "(" + fingerprint_type + "(%s), " + fingerprint_column + ")"},
                select_params=(structure,),
                where=[
                    fingerprints_table + "." + fingerprints_pk + " = " + self.model._meta.db_table + "." +
                    molregno_column + " and " + fingerprint_type + "(%s) %% " + fingerprint_column + " and " +
                    similarity_type + "(" + fingerprint_type + "(%s), " + fingerprint_column + ") between %s and 1.0"
                ],
                tables=[fingerprints_table],
                order_by=['-similarity'],
                params=[structure, structure, (sim / 100.0)])
            cursor.execute('set rdkit.tanimoto_threshold=0.5;')
            return ret
        else:
            raise NotImplementedError

    def with_substructure(self, structure):
        ctab_column = self.get_column('ctab')
        connection = connections[self._db or 'default']
        if connection.vendor == 'oracle':
            return self.extra(where=["(sss(" + ctab_column + ",%s)=1)"], params=('smiles:' + structure,))
        if connection.vendor == 'postgresql':
            return self.extra(where=[ctab_column + "@>%s"], params=(structure,))
        else:
            raise NotImplementedError

    def flexmatch(self, structure):
        ctab_column = self.get_column('ctab')
        connection = connections[self._db or 'default']
        if connection.vendor == 'oracle':
            return self.extra(
                where=["(flexmatch(" + ctab_column + ",%s,'ignore=all')=1)"], params=('smiles:' + structure,))
        if connection.vendor == 'postgresql':
            try:
                from rdkit.rdBase import rdkitVersion
            except Exception:
                rdkitVersion = 0
            if rdkitVersion != '2015.03.1':
                return self.extra(where=[ctab_column + "@=%s"], params=(structure,))
            return self.extra(
                where=[ctab_column + "@>%s" + " AND " + ctab_column + "<@%s"], params=(structure, structure))
        else:
            raise NotImplementedError

#-----------------------------------------------------------------------------------------------------------------------

class CompoundMolsQuerySet(QuerySet,CompoundMolsMixin):
    pass

#-----------------------------------------------------------------------------------------------------------------------

class CompoundMolsManager(models.Manager, CompoundMolsMixin):
    use_for_related_fields = True
    def get_query_set(self):
        return CompoundMolsQuerySet(self.model, using=self._db)

#-----------------------------------------------------------------------------------------------------------------------