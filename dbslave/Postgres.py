########################################################################################################################
#    File: Postgres.py
# Purpose: Class to make working with Postgres a lot easier.
#  Author: Dan Huckson, https://github.com/unodan
########################################################################################################################
import os
import psycopg2
import logging as lg
from dbslave.interface import *
from time import gmtime, strftime




class Interface(Instance):
    def __init__(self, credentials, log_name=None):
        self.host = None
        self.conn = None
        self.cursor = None
        self.dbName = None
        self.dbUser = None
        self.dbPassword = None
        self.credentials = credentials
        self.class_name = self.__class__.__name__

        if not log_name:
            log_name = self.class_name + '.log'

        lg.basicConfig(filename=log_name, format='%(levelname)s:%(name)s:%(asctime)s:%(message)s',
            datefmt='%Y/%m/%d %I:%M:%S', level=lg.DEBUG)
        lg.info(self.class_name + ':__init__:Object created')

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
            os.popen('pg_dump -U %s %s | gzip -c > %s.gz' %
                (self.dbUser, database_name, database_name + '_' + date_stamp))

            lg.info('dump:' + database_name + '_' + date_stamp + '.gz')
            return True

        except Exception as err:
            lg.error('dump:' + str(err))
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
            lg.info('commit')
            return True

        except Exception as err:
            lg.error('commit:' + str(err))
            return False

    def connect(self, database_name=None):
        cred = self.credentials

        if database_name:
            self.dbName = database_name
        else:
            self.dbName = cred['database']

        try:
            self.conn = psycopg2.connect(host=cred['host'], port=cred['port'],
                user=cred['user'], database=database_name, password=cred['password'])
            self.conn.autocommit = True

            self.host = cred['host']
            self.dbUser = cred['user']
            self.dbPassword = cred['password']
            self.cursor = self.conn.cursor()

            lg.info('connect:Connection authenticated for user:' + cred['user'])
            return True

        except Exception as err:
            self.dbName = None
            lg.error('connect:' + str(err))
            return False

    def execute(self, sql, args=None):
        try:
            if not args:
                self.cursor.execute(sql)
            else:
                self.cursor.execute(sql, args)

            lg.info('execute:' + sql)
            return True

        except Exception as err:
            lg.error('execute:'+str(err) + ':' + sql)
            return False

    def fetchone(self):
        try:
            row = self.cursor.fetchone()
            lg.info('fetchone')
            return row

        except Exception as err:
            lg.error('fetchone:'+str(err))
            return False

    def fetchall(self):
        try:
            rows = self.cursor.fetchall()
            lg.info('fetchall')
            return rows

        except Exception as err:
            lg.error('fetchall:'+str(err))
            return False

    def drop_table(self, table_name):
        sql = 'DROP TABLE ' + table_name + ';'
        try:
            self.cursor.execute(sql)
            lg.info('drop_table:' + sql)
            return True

        except Exception as err:
            lg.error('drop_table:' + str(err) + ':' + sql)
            return False

    def drop_database(self, database_name):
        sql = 'DROP DATABASE "' + database_name + '";'
        try:
            self.cursor.execute(sql)
            lg.info('drop_database:' + sql)
            return True

        except Exception as err:
            lg.error('drop_database:' + str(err) + ':' + sql)
            return False

    def create_table(self, table_name, sql):
        sql = 'CREATE TABLE ' + table_name + ' ( ' + sql + ' );'
        try:
            self.cursor.execute(sql)
            lg.info('create_table:' + sql)
            return True

        except Exception as err:
            lg.error('create_table:' + str(err) + ':' + sql)
            return False

    def create_database(self, database_name):
        sql = 'CREATE DATABASE "%s";' % database_name
        try:
            self.cursor.execute(sql)
            lg.info('create_database:' + sql)
            return True

        except Exception as err:
            lg.error('create_database:' + str(err) + ':' + sql)
            return False

    def row_exist(self, table_name, id):
        sql = 'SELECT id FROM ' + table_name + ' WHERE id=%s;'
        if self.execute(sql, (id,)):
            if self.fetchone():
                return True
            else:
                return False

    def table_exist(self, table_name):
        sql = "SELECT EXISTS(SELECT 1 FROM information_schema.tables " \
              "WHERE table_catalog=%s AND table_name=%s AND table_schema='public');"
        try:
            self.cursor.execute(sql, (self.dbName, table_name))
            if self.cursor.fetchone()[0]:
                return True
            else:
                return False

        except Exception as err:
            print("TABLE EXISTS ERROR")
            lg.error('table_exist:' + str(err))
            return False

    def database_exist(self, database_name):
        sql = "SELECT exists(SELECT 1 from pg_catalog.pg_database where datname = %s)"
        try:
            self.cursor.execute(sql, (database_name,))
            return self.cursor.fetchone()[0]

        except Exception as err:
            lg.error('database_exist:' + str(err))
            return False

    def insert_row(self, table, row):
        parts = ''
        sql = 'INSERT INTO ' + table + ' ('
        for f in row:
            parts += (f + ',')
        sql = sql + parts[:-1] + ') VALUES ('

        parts = ''
        for f in row:
            parts += '%s,'
        sql = sql + parts[:-1] + ');'

        data = []
        for c in row:
            data.append(row[c])

        try:
            self.cursor.execute(sql, tuple(data))
            lg.info('insert_row:' + sql)
            return True

        except Exception as err:
            lg.error('insert_row:'+str(err) + ':' + sql)
            return False

    def update_row(self, table, row, id):
        parts = ''
        sql = 'UPDATE ' + table + ' SET '
        for f in row:
            parts += (f + '=%s,')
        sql = sql + parts[:-1] + ' WHERE id=%s;'

        data = []
        for c in row:
            data.append(row[c])
        data.append(id)

        try:
            self.cursor.execute(sql, tuple(data))
            lg.info('update_row:' + sql)
            return True

        except Exception as err:
            lg.error('update_row:'+str(err) + ':' + sql)
            return False

    def delete_row(self, table, id):
        sql = 'DELETE FROM ' + table + ' WHERE id = %s;'
        try:
            self.cursor.execute(sql, (id,))
            lg.info('delete_row:' + sql)
            return True

        except Exception as err:
            lg.error('delete_row:'+str(err) + ':' + sql)
            return False

