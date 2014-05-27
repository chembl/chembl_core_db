from django.db.backends.oracle.introspection import DatabaseIntrospection as OracleDatabaseIntrospection
import cx_Oracle
import re

#-----------------------------------------------------------------------------------------------------------------------

IN_REGEX = re.compile(r'(\S+) in (\([^\)]+\))')
IS_NOT_NULL_REGEX = re.compile(r'(\S+) is not null')
IS_POSITIVE_REGEX = re.compile(r'(\S+) >= 0')
DISTINCT_CUTOFF = 10

#-----------------------------------------------------------------------------------------------------------------------

class DatabaseIntrospection(OracleDatabaseIntrospection):

    data_types_reverse = {
        cx_Oracle.CLOB: 'ChemblTextField',
        cx_Oracle.NCLOB: 'ChemblTextField',
        cx_Oracle.BLOB: 'BlobField',
        cx_Oracle.DATETIME: 'ChemblDateField',
        cx_Oracle.STRING: 'ChemblCharField',
        cx_Oracle.FIXED_CHAR: 'ChemblCharField',

        cx_Oracle.NUMBER: 'DecimalField',
        cx_Oracle.TIMESTAMP: 'DateTimeField',
        }

#-----------------------------------------------------------------------------------------------------------------------

    def get_field_type(self, data_type, description):
        #print description
        constraints = description[7]
        precision = description[4]
        scale = description[5]
        ins = [c for c in constraints if IN_REGEX.match(c)]
        if data_type == cx_Oracle.NUMBER:
            if precision == 0 and scale == (-127):
                return 'ChemblNoLimitDecimalField'
            if precision == 1 and scale == 0:
                if ins and any(map(lambda x : IN_REGEX.search(x).groups()[1] == '(0,1)', ins)):
                        if description[6] == 0 or any(map(lambda x : IS_NOT_NULL_REGEX.match(x), constraints)):
                            return 'ChemblBooleanField'
                        return 'ChemblNullableBooleanField'
                elif ins and any(map(lambda x : IN_REGEX.search(x).groups()[1] == '(-1,0,1)', ins)):
                    return 'ChemblNullBooleanField'
            if description[5] == 0:
                if any(map(lambda x:IS_POSITIVE_REGEX.match(x), constraints)) or any(map(lambda x :
                                                            IN_REGEX.search(x).groups()[1].startswith('(0,'), ins)):
                    return 'ChemblPositiveIntegerField'
                return 'ChemblIntegerField'
            if any(map(lambda x:IS_POSITIVE_REGEX.match(x), constraints)):
                return 'ChemblPositiveDecimalField'
            return 'DecimalField'
        else:
            return super(DatabaseIntrospection, self).get_field_type(
                data_type, description)

#-----------------------------------------------------------------------------------------------------------------------

    def get_table_description(self, cursor, table_name):
        "Returns a description of the table, with the DB-API cursor.description interface."
        cursor.execute("SELECT * FROM %s WHERE ROWNUM < 2" % self.connection.ops.quote_name(table_name))
        description = []
        for desc in cursor.description:
            description.append((desc[0].lower(),) + desc[1:])
        #print str(description)
        return description

#-----------------------------------------------------------------------------------------------------------------------

    def get_sequence_incrementing_triggers(self, cursor, table_name):
        cursor.execute("""
    select trigger_name
    from USER_TRIGGERS
    where table_name=%s
      and status='ENABLED'
      and triggering_event='INSERT';""", [table_name.upper()])

        triggers = []
        for row in cursor.fetchall():
            triggers.append(row[0])
        return triggers

#-----------------------------------------------------------------------------------------------------------------------

    def get_distinct_values(self, cursor, table_name, column_name):
        values = []
        cursor.execute("""
    select distinct %s
    from %s
    order by %s;""" % (column_name.upper(), table_name.upper(), column_name.upper()))
        for row in cursor.fetchall():
            values.append(row[0])
        return values

#-----------------------------------------------------------------------------------------------------------------------

    def is_positive(self, cursor, table_name, column_name):
        values = []
        cursor.execute("""
    select min(%s)
    from %s;""" % (column_name.upper(), table_name.upper()))
        val = cursor.fetchone()
        return val[0] >= 0

#-----------------------------------------------------------------------------------------------------------------------

    def get_unique_together(self, cursor, table_name):
        cursor.execute("""
    SELECT i.index_name,
       c.column_name,
       d.data_length
  FROM user_indexes i,
       user_cons_columns c,
       user_tab_cols d
 WHERE i.table_name  = %s
   AND i.uniqueness  = 'UNIQUE'
   AND i.index_name  = c.constraint_name
   AND i.table_owner = c.owner
   AND i.table_name  = c.table_name
   AND c.column_name = d.column_name
   AND c.table_name = d.table_name
   AND c.constraint_name IN (SELECT constraint_name FROM user_cons_columns WHERE position = 2)
 ORDER BY i.index_name, c.position;""", [table_name.upper()])

        uniques = {}
        sum = 0
        for row in cursor.fetchall():
            sum += int(row[2])
            if row[0] in uniques:
                uniques[row[0]].append(row[1])
            else:
                uniques[row[0]] = [row[1]]
        if sum >= 767:
            print '\033[0m\033[1;%dmWarning [%s]: Uniqe together index for these columns may be to long for MySQL capabilities: %s' % (34, 5, str(uniques))
        return uniques

#-----------------------------------------------------------------------------------------------------------------------

    def get_arity(self, cursor, table_name):
        table_name = table_name.upper()
        cursor.execute("""
SELECT  column_name, num_distinct, nullable, data_type, char_length, column_id
FROM    user_tab_columns t
WHERE   t.table_name = %s
AND data_type NOT IN ('BLOB', 'CLOB', 'NCLOB', 'DATETIME', 'TIMESTAMP', 'DATE')
AND (data_scale = 0 OR data_scale is null);""", [table_name])

        arity = {}
        for row in cursor.fetchall():
            arity[row[0].lower()] = (row[1], row[2], row[3], row[4], row[5] - 1)
        return arity

#-----------------------------------------------------------------------------------------------------------------------

    def get_choices(self, constraints, arity, cursor, table_name, comments):
        ar = filter(lambda x: arity[x][0] > 1 and arity[x][0] <= DISTINCT_CUTOFF, arity)
        ins = [c for c in constraints if IN_REGEX.match(c)]
        res = map(lambda x: (IN_REGEX.search(x).groups()[0], IN_REGEX.search(x).groups()[1][1:-1]), ins)
        ret = {}
        null_flags = []
        flags = []
        tri_state_flags = []
        for r in res:
            choices = []
            try:
                for choice in r[1].split(','):
                    ch = choice.strip()
                    if ch.startswith("'"):
                        ch = str(ch[1:-1].upper())
                    else:
                        ch = int(ch)
                    choices.append((ch, str(ch)))
            except Exception as e:
                print constraints
                print "exception for choices: --->%s<---- (%s)" % (r[1], res)
                raise e
            ret[r[0]] = tuple(choices)

        choices_without_constraints = filter(lambda x: x not in ret.keys() ,ar)
        for ch in choices_without_constraints:
            distinct = self.get_distinct_values(cursor, table_name, ch)
            no_null_distinct = [x for x in distinct if x != None]
            if not no_null_distinct:
                print "\033[0m\033[1;%dmWarning [%s]: column %s.%s is empty\033[0m" % (30, 1, table_name, ch)
                continue
            if 'CHAR' in arity[ch][2]:
                max_len  = max(map(lambda x:len(x) if x else 0, no_null_distinct))
                if  max_len *2 < arity[ch][3]:
                    print "\033[0m\033[1;%dmWarning [%s]: Longest string in %s.%s is more than two times shorter (%s) than maximal allowed length (%s)\033[0m" % (35, 6, table_name, ch, max_len, arity[ch][3])
            if len(distinct) == 2 and len(no_null_distinct) == 1:
                if no_null_distinct[0] == 1:
                    null_flags.append(ch)
                    if not ch.endswith('_flag') and not comments.get(ch, '').lower().startswith('flag'):
                        print "\033[0m\033[1;%dmWarning [%s]: column %s.%s has only two distinct values (NULL, %s) - should it be a flag?\033[0m" % (36, 7, table_name, ch, str(no_null_distinct[0]))
                else:
                    print "\033[0m\033[1;%dmWarning [%s]: column %s.%s has only two distinct values (NULL, %s) - should it be a flag?\033[0m" % (36, 7, table_name, ch, str(no_null_distinct[0]))
                continue
            if len(no_null_distinct) > 2:
                if len(no_null_distinct) == 3 and no_null_distinct[0] == -1 and no_null_distinct[1] == 0 and no_null_distinct[2] == 1:
                    tri_state_flags.append(ch)
                    if ch.endswith('_flag') and not comments.get(ch, '').lower().startswith('flag'):
                        print "\033[0m\033[1;%dmWarning [%s]: column %s.%s has only %s distinct values (%s) but no constraint.\033[0m" % (37, 8, table_name, ch, len(no_null_distinct), ', '.join(map(lambda x: str(x),no_null_distinct)))
                else:
                    if not ch.endswith(('_type', '_lookup', '_version', '_by')):
                        print "\033[0m\033[1;%dmWarning [%s]: column %s.%s has only %s distinct values (%s) but no constraint.\033[0m" % (37, 8, table_name, ch, len(no_null_distinct), ', '.join(map(lambda x: str(x),no_null_distinct)))
                    if 'CHAR' in arity[ch][2]:
                        ret[ch] = tuple(map(lambda x: (str(x), str(x)), no_null_distinct))
                    else:
                        ret[ch] = tuple(map(lambda x: (x, str(x)), no_null_distinct))
            elif len(no_null_distinct) == 2:
                if no_null_distinct[0] == 0 and no_null_distinct[1] == 1:
                    if arity[ch][1] == 'Y':
                        null_flags.append(ch)
                    else:
                        flags.append(ch)
                    if not ch.endswith('_flag') and not comments.get(ch, '').lower().startswith('flag'):
                        print "\033[0m\033[1;%dmWarning [%s]: column %s.%s has only two distinct values (%s) - should it be a flag?\033[0m" % (30, 9, table_name, ch, ', '.join(map(lambda x: str(x),no_null_distinct)))
                else:
                    print "\033[0m\033[1;%dmWarning [%s]: column %s.%s has only two distinct values (%s) - should it be a flag?\033[0m" % (30, 9, table_name, ch, ', '.join(map(lambda x: str(x),no_null_distinct)))
                    if 'CHAR' in arity[ch][2]:
                        ret[ch] = tuple(map(lambda x: (str(x), str(x)), no_null_distinct))
                    else:
                        ret[ch] = tuple(map(lambda x: (x, str(x)), no_null_distinct))
        return ret, null_flags, flags, tri_state_flags

#-----------------------------------------------------------------------------------------------------------------------

    def get_defaults(self, cursor, table_name):
        table_name = table_name.upper()
        cursor.execute("""
    SELECT COLUMN_NAME, DATA_DEFAULT, DATA_TYPE
    FROM USER_TAB_COLUMNS
    WHERE TABLE_NAME = %s AND DATA_DEFAULT IS NOT NULL;""", [table_name])

        defaults = {}
        for row in cursor.fetchall():
            if row[1] == 'NULL':
                continue
            if row[2] == 'NUMBER':
                try:
                    defaults[row[0].lower()] = int(row[1])
                except:
                    continue
            else:
                default = row[1].strip()
                if default.startswith("'") and default.endswith("'"):
                    default = default[1:-1]
                defaults[row[0].lower()] = default
        return defaults

#-----------------------------------------------------------------------------------------------------------------------

    def get_contraints(self, cursor, table_name):
        table_name = table_name.upper()
        cursor.execute("""
    select search_condition
    from user_constraints
    where table_name=%s and constraint_type='C';""", [table_name])

        constraints = []
        for row in cursor.fetchall():
            constraints.append(row[0].lower())
        return constraints

#-----------------------------------------------------------------------------------------------------------------------

    def get_comments(self, cursor, table_name):
        table_name = table_name.upper()
        cursor.execute("""
    SELECT lower(tc.column_name) as column_name
    ,      cc.comments
    FROM   user_col_comments cc
    JOIN   user_tab_columns  tc on  cc.column_name = tc.column_name
                                and cc.table_name  = tc.table_name
    WHERE  cc.table_name = %s AND cc.comments IS NOT NULL;""", [table_name])

        comments = {}
        for row in cursor.fetchall():
            if 'deprecated' in row[1].lower():
                print "\033[0m\033[1;%dmWarning [%s]: column %s.%s may be deprecated \033[0m" % (32, 10, table_name, row[0])
            comments[row[0]] = (row[1])
        return comments

#-----------------------------------------------------------------------------------------------------------------------

    def get_relations(self, cursor, table_name):
        """
        Returns a dictionary of {field_index: (field_index_other_table, other_table)}
        representing all relationships to the given table. Indexes are 0-based.
        """
        table_name = table_name.upper()
        cursor.execute("""
    SELECT ta.column_id - 1, tb.table_name, tb.column_id - 1
    FROM   user_constraints, USER_CONS_COLUMNS ca, USER_CONS_COLUMNS cb,
           user_tab_cols ta, user_tab_cols tb
    WHERE  user_constraints.table_name = %s AND
           ta.table_name = %s AND
           ta.column_name = ca.column_name AND
           ca.table_name = %s AND
           user_constraints.constraint_name = ca.constraint_name AND
           user_constraints.r_constraint_name = cb.constraint_name AND
           cb.table_name = tb.table_name AND
           cb.column_name = tb.column_name AND
           ca.position = cb.position""", [table_name, table_name, table_name])

        relations = {}
        for row in cursor.fetchall():
            relations[row[0]] = (row[2], row[1].lower())
        return relations

#-----------------------------------------------------------------------------------------------------------------------

    def get_nonunique_indexes(self, cursor, table_name):
        table_name = table_name.upper()
        cursor.execute("""
     select column_name
 from user_indexes i,
      user_ind_columns c
 where c.table_name=%s
    and c.table_name = i.table_name
    and c.index_name = i.index_name
    and uniqueness='NONUNIQUE';""", [table_name])

        indexes = []
        for row in cursor.fetchall():
            indexes.append(row[0].lower())
        return indexes

#-----------------------------------------------------------------------------------------------------------------------

    def get_indexes(self, cursor, table_name):
        """
        Returns a dictionary of fieldname -> infodict for the given table,
        where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index}
        """
        # This query retrieves each index on the given table, including the
        # first associated field name
        # "We were in the nick of time; you were in great peril!"
        sql = """\
SELECT LOWER(all_tab_cols.column_name) AS column_name,
       CASE user_constraints.constraint_type
           WHEN 'P' THEN 1 ELSE 0
       END AS is_primary_key,
       CASE user_indexes.uniqueness
           WHEN 'UNIQUE' THEN 1 ELSE 0
       END AS is_unique
FROM   all_tab_cols, user_cons_columns, user_constraints, user_ind_columns, user_indexes
WHERE  all_tab_cols.column_name = user_cons_columns.column_name (+)
  AND  all_tab_cols.table_name = user_cons_columns.table_name (+)
  AND  user_cons_columns.constraint_name = user_constraints.constraint_name (+)
  AND  user_constraints.constraint_type (+) = 'P'
  AND  user_ind_columns.column_name (+) = all_tab_cols.column_name
  AND  user_ind_columns.table_name (+) = all_tab_cols.table_name
  AND  user_indexes.uniqueness (+) = 'UNIQUE'
  AND  user_indexes.index_name (+) = user_ind_columns.index_name
  AND  all_tab_cols.table_name = UPPER(%s)
"""
        cursor.execute(sql, [table_name])
        indexes = {}
        for row in cursor.fetchall():
            if not row[0] in indexes:
                indexes[row[0]] = {'primary_key': row[1], 'unique': row[2]}
            else:
                primary = indexes[row[0]]['primary_key']
                unique = indexes[row[0]]['unique']

                if not primary and row[1]:
                    indexes[row[0]]['primary_key'] = row[1]
                if not unique and row[2]:
                    indexes[row[0]]['unique'] = row[2]
        return indexes

#-----------------------------------------------------------------------------------------------------------------------