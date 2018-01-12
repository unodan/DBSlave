########################################################################################################################
#    File: Sqlite3.py
# Purpose: Class to make working with SQLite3 a lot easier.
#  Author: Dan Huckson, https://github.com/unodan
########################################################################################################################
import gzip
import sqlite3
import logging as lg
from os import remove
from os.path import isfile, getsize
from time import gmtime, strftime
from dbslave.interface import *


class Interface(Instance):
    def __init__(self, credentials, log_name=None):

        self.conn = None
        self.cursor = None
        self.dbName = None
        self.credentials = credentials
        self.class_name = self.__class__.__name__

        if not log_name:
            log_name = self.class_name + '.log'

        lg.basicConfig(filename=log_name, format='%(levelname)s:%(name)s:%(asctime)s:%(message)s',
            datefmt='%Y/%m/%d %H:%M:%S', level=lg.DEBUG)
        lg.info('__init__:SQLite3 Object created')

    def use(self, database_name):
        cred = self.credentials
        try:
            self.connect(database_name)
            lg.info(self.class_name + ':use:Using database:' + database_name + ':IMPLICIT SQL')
            return True

        except Exception as err:
            lg.error('use:' + str(err) + ':IMPLICIT SQL')
            return False

    def dump(self, database_name):
        try:
            date_stamp = strftime("_%Y-%m-%d_%H:%M:%S", gmtime())
            with open(database_name + date_stamp + '.sql', 'w') as f:
                for i in self.conn.iterdump():
                    f.write("%s\n" % i)
            f.close()

            with open(database_name + date_stamp + '.sql', 'rb') as f_in, \
                    gzip.open(database_name + date_stamp + '.gz', 'wb') as f_out:
                f_out.writelines(f_in)
            f_out.close()

            remove(database_name + date_stamp + '.sql')

            return True

        except Exception as err:
            lg.error(self.class_name + ':dump:' + str(err))
            return False

    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
            self.conn = None
            self.cursor = None
            lg.info('close:Closed database:' + self.dbName)
            return True

        except Exception as err:
            lg.error('close:' + str(err))
            return False

    def commit(self):
        try:
            self.conn.commit()
            lg.info('commit:Successful')
            return True

        except Exception as err:
            lg.error(self.class_name + ':commit:' + str(err))
            return False

    def connect(self, database_name=None):
        cred = self.credentials

        if database_name:
            self.dbName = database_name
        else:
            self.dbName = cred['database']

        try:
            self.conn = sqlite3.connect(self.dbName)
            self.cursor = self.conn.cursor()
            lg.info('connect:Successful')
            return True

        except Exception as err:
            self.dbName = None
            lg.error(self.class_name + ':connect:' + str(err))
            return False

    def execute(self, sql, args=None):
        sql = sql.replace('%s', '?')
        try:
            if not args:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, args)

            lg.info('execute:Successful:' + sql)
            return True

        except Exception as err:
            lg.error('execute:'+str(err) + ':' + sql)
            return False

    def fetchone(self):
        try:
            row = self.cursor.fetchone()
            lg.info('fetchone:Successful')
            return row

        except Exception as err:
            lg.error('fetchone:'+str(err))
            return False

    def fetchall(self):
        try:
            rows = self.cursor.fetchall()
            lg.info('fetchall:Successful')
            return rows

        except Exception as err:
            lg.error('fetchall:'+str(err))
            return False

    def drop_table(self, table_name):
        sql = 'DROP TABLE ' + table_name + ';'
        try:
            self.cursor.execute(sql)
            lg.info('drop_table:Successful:' + sql)
            return True

        except Exception as err:
            lg.error('drop_table:' + str(err) + ':' + sql)
            return False

    def drop_database(self, database_name):
        try:
            if self.database_exist(database_name):
                remove(database_name)
                lg.info('drop_database:Successful:')
                return True
            else:
                raise ValueError('Database not found.', database_name)

        except Exception as err:
            lg.error('drop_database:' + str(err))
            return False

    def create_table(self, table_name, sql):
        sql = 'CREATE TABLE ' + table_name + ' ( ' + sql + ' );'
        try:
            self.cursor.execute(sql)
            lg.info('create_table:Successful:' + sql)
            return True

        except Exception as err:
            lg.error('create_table:' + str(err) + ':' + sql)
            return False

    def create_database(self, database_name):
        try:
            if not isfile(database_name):
                open(database_name, 'w').close()
                lg.info('create_database:Successful:Database:' + self.dbName)
                return True

            elif not getsize(database_name):
                lg.info('create_database:Successful:Database:' + self.dbName)
                return True

            else:
                with open(database_name, 'r') as fd:
                    header = fd.read(100)

                    if header[:15] == 'SQLite format 3':
                        raise ValueError('Database file already exists.', database_name)
                    else:
                        raise ValueError('File is not a SQLite3 database file.', database_name)

        except Exception as err:
            lg.error('create_database:' + str(err))
            return False

    def row_exist(self, table_name, id):
        sql = 'SELECT id FROM ' + table_name + ' WHERE id=?;'
        if self.execute(sql, (id,)):
            if self.fetchone():
                return True
            else:
                return False

    def table_exist(self, table_name):
        sql = 'SELECT 1 FROM sqlite_master ' \
              'WHERE type="table" AND name="?";'
        try:
            self.cursor.execute(sql, (table_name,))
            if self.cursor.fetchone():
                return True
            else:
                return False

        except Exception as err:
            lg.error('table_exist:' + str(err))
            return False

    def database_exist(self, database_name):
        try:
            if not isfile(database_name):
                raise ValueError('File not found.', database_name)

            elif not getsize(database_name):
                return True

            elif getsize(database_name) < 100 and getsize(database_name):
                raise ValueError('1File is not a SQLite3 database file.', database_name)
            else:
                with open(database_name, 'rb') as fd:
                    header = fd.read(100)

                    if header[:15] == b'SQLite format 3':
                        return True

            raise ValueError('2File is not a SQLite3 database file.', database_name)

        except Exception as err:
            lg.error('database_exist:Failed:' + str(err))
            return False

    def insert_row(self, table_name, row):
        parts = ''
        sql = 'INSERT INTO ' + table_name + ' ('
        for f in row:
            parts += (f + ',')
        sql = sql + parts[:-1] + ') VALUES ('

        parts = ''
        for f in row:
            parts += '?,'
        sql = sql + parts[:-1] + ');'

        data = []
        for c in row:
            data.append(row[c])

        try:
            self.cursor.execute(sql, tuple(data))
            lg.info('insert_row:Successful:' + sql)
            return True

        except Exception as err:
            lg.error('insert_row:'+str(err) + ':' + sql)
            return False

    def update_row(self, table_name, row, id):
        parts = ''
        sql = 'UPDATE ' + table_name + ' SET '
        for f in row:
            parts += (f + '=?,')
        sql = sql + parts[:-1] + ' WHERE id=?;'

        data = []
        for c in row:
            data.append(row[c])
        data.append(id)

        try:
            self.cursor.execute(sql, tuple(data))
            lg.info('update_row:Successful:' + sql)
            return True

        except Exception as err:
            lg.error('update_row:'+str(err) + ':' + sql)
            return False

    def delete_row(self, table_name, id):
        sql = 'DELETE FROM ' + table_name + ' WHERE id = ?;'
        try:
            self.cursor.execute(sql, (id,))
            lg.info('delete_row:Successful:' + sql)
            return True

        except Exception as err:
            lg.error('delete_row:'+str(err) + ':' + sql)
            return False
