########################################################################################################################
#    File: Maria.py
# Purpose: Class to make working with MariaDB/MySQL a lot easier.
#  Author: Dan Huckson, https://github.com/unodan
########################################################################################################################
import os
import time
import pymysql
import logging as lg
from dbslave.interface import *




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
        sql = 'USE ' + database_name

        try:
            self.cursor.execute(sql)
            lg.info(self.class_name + ':use:Using database:' + database_name + ':IMPLICIT SQL')
            return True

        except Exception as err:
            lg.error('use:' + str(err) + ':IMPLICIT SQL')
            return False

    def dump(self, database_name):
        try:
            t = time.strftime('%Y-%m-%d_%H:%M:%S')
            os.popen('mysqldump -u %s -p%s -h %s -e --opt -c %s | gzip -c > %s.gz' % (
                self.dbUser, self.dbPassword, self.host, database_name, database_name + '_' + t))
            lg.info('dump:' + database_name + '_' + t + '.gz')
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
            self.conn = pymysql.connect(host=cred['host'], port=cred['port'], user=cred['user'],
                passwd=cred['password'], database=database_name, charset=cred['charset'])

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
        sql = 'DROP DATABASE ' + database_name + ';'
        try:
            self.use('master')
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
        sql = 'CREATE DATABASE %s;' % database_name
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
        sql = 'SELECT 1 FROM information_schema.tables ' \
              'WHERE table_schema="%s" AND table_name="%s";'
        try:
            self.cursor.execute(sql, (self.dbName, table_name))
            if self.cursor.fetchone():
                return True
            else:
                return False

        except Exception as err:
            lg.error('table_exist:' + str(err))
            return False

    def database_exist(self, database_name):
        sql = 'SHOW DATABASES;'
        try:
            self.cursor.execute(sql)
            for r in self.cursor.fetchall():
                if database_name in r:
                    return True

            return False

        except Exception as err:
            lg.error('database_exist:' + str(err))
            return False

    def insert_row(self, table_name, row):
        parts = ''
        sql = 'INSERT INTO ' + table_name + ' ('
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

    def update_row(self, table_name, row, id):
        parts = ''
        sql = 'UPDATE ' + table_name + ' SET '
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

    def delete_row(self, table_name, id):
        sql = 'DELETE FROM ' + table_name + ' WHERE id = %s;'
        try:
            self.cursor.execute(sql, (id,))
            lg.info('delete_row:' + sql)
            return True

        except Exception as err:
            lg.error('delete_row:'+str(err) + ':' + sql)
            return False

