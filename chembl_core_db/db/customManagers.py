__author__ = 'mnowotka'

from django.db import models
from django.db import connections
from django.db.models.query import QuerySet

#-----------------------------------------------------------------------------------------------------------------------

class CompoundMolsMixin(object):

    def get_column(self, name):
        return filter(lambda x: x.name == name, self.model._meta.fields)[0].db_column or name

    def similar_to(self, structure, similarity_index):
        ctab_column = self.get_column('ctab')
        molregno_column = self.get_column('molecule')
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
            cursor = connection.cursor()
            cursor.execute("select 'c1ccccc1O'::mol;") # dirty hack but what can I do...
            cursor.execute('set rdkit.tanimoto_threshold=%s;', (sim / 100.0,))
            ret = self.extra(select={'similarity': "tanimoto_sml(torsionbv_fp(%s), torsionbv)"},
                select_params=(structure,),
                where=["fps_rdkit.molregno = " + self.model._meta.db_table + "." + molregno_column +
                       " and torsionbv_fp(%s) %% torsionbv and tanimoto_sml(torsionbv_fp(%s), torsionbv) between %s and 1.0"],
                tables=['fps_rdkit'],
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
            return self.extra(where=["(sss(" + ctab_column + ",%s,'ignore=all')=1)"], params=('smiles:' + structure,))
        if connection.vendor == 'postgresql':
            return self.extra(where=[ctab_column + "@>%s"], params=(structure,))
        else:
            raise NotImplementedError

    def flexmatch(self, structure):
        ctab_column = self.get_column('ctab')
        connection = connections[self._db or 'default']
        if connection.vendor == 'oracle':
            return self.extra(where=["(flexmatch(" + ctab_column + ",%s,'ignore=all')=1)"], params=('smiles:' + structure,))
        if connection.vendor == 'postgresql':
            return self.extra(where=[ctab_column + "@=%s"], params=(structure,))
        else:
            raise NotImplementedError

#-----------------------------------------------------------------------------------------------------------------------

class CompoundMolsQuerySet(QuerySet,CompoundMolsMixin):
    pass

#-----------------------------------------------------------------------------------------------------------------------

class CompoundMolsManager(models.Manager, CompoundMolsMixin):
    def get_query_set(self):
        return CompoundMolsQuerySet(self.model, using=self._db)

#-----------------------------------------------------------------------------------------------------------------------